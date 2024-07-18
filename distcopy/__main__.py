import subprocess
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, List, Iterable

import pandas as pd
from tqdm import tqdm


def copy_rsync(src_node: str, src: str, dst_node: str, dst: str, files_from_source: Optional[Iterable[str]] = None) -> dict:
    """
    Copy files from source to destination using rsync.

    :param src_node: node where the source path is located
    :param src: source path
    :param dst_node: node where the destination path is located
    :param dst: destination path
    :param files_from_source: allows to specify files to copy from source (uses relative paths)
    :return: message with success or error
    """
    if files_from_source is None:
        cmd = ["ssh", dst_node, f"mkdir -p {str(Path(dst).parent)} && rsync -a {src_node}:{src} {dst}"]
        print("Running", cmd)
        r = subprocess.run(cmd)
    else:
        cmd = ["ssh", dst_node, f"mkdir -p {str(Path(dst).parent)} && rsync -a --files-from=- {src_node}:{src} {dst}"]
        print("Running", cmd)
        r = subprocess.run(cmd, input="\n".join(files_from_source), text=True)

    if r.returncode == 0:
        return {
            "message": "Success"
        }
    else:
        return {
            "error": "Error during copy"
        }


def list_all_files_in_folder(run_on: str, folder: str) -> Optional[List[str]]:
    """
    List all files recursively in folder.

    :param run_on: node where the command should be executed
    :param folder: folder path
    :return: list of files in folder
        the paths are relative to the folder
    """
    r = subprocess.run(["ssh", run_on, f"find {folder} -type f"], capture_output=True)

    if r.returncode == 0:
        folder = Path(folder)
        paths = r.stdout.decode().split("\n")
        return [str(Path(p).relative_to(folder)) for p in paths if p]
    else:
        return None


def is_file(run_on: str, path: str) -> bool:
    """
    Check if path is a file.

    :param run_on: node where the command should be executed
    :param path: path to check
    :return: True if path is a file, False otherwise
    """
    r = subprocess.run(["ssh", run_on, f"test -f {path}"], capture_output=True)

    return r.returncode == 0


def number_of_lines_in_file(run_on: str, path: str) -> int:
    """
    Get number of lines in file.

    :param run_on: node where the command should be executed
    :param path: path to file
    :return: number of lines in file
    """
    r = subprocess.run(["ssh", run_on, f"wc -l {path}"], capture_output=True)

    if r.returncode == 0:
        return int(r.stdout.decode().split()[0])
    else:
        return 0


def remove_file(run_on: str, path: str) -> dict:
    """
    Remove file.

    :param run_on: node where the command should be executed
    :param path: path to file
    :return: message with success or error
    """
    r = subprocess.run(["ssh", run_on, f"rm {path}"])

    if r.returncode == 0:
        return {
            "message": "Success"
        }
    else:
        return {
            "error": "Error during file removal"
        }


def send_line_interval_from_file(src_node: str, src: str, dest_node: str, dest: str, from_line: int = 0,
                                 to_line: Optional[int] = None, append: bool = False) -> dict:
    """
    Send interval [from_line, to_line) of lines from source file to destination.

    :param src_node: node where the source path is located
    :param src: source path
    :param dest_node: node where the destination path is located
    :param dest: destination path
    :param from_line: start line offset (0-based)
    :param to_line: end line offset (if None then to the end of file)
    :param append: if True then append to the destination file
    :return: message with success or error
    """

    pipe_type = ">>" if append else ">"

    if from_line == 0 and to_line is None:
        cmd = ["ssh", src_node, f"cat {src} | ssh {dest_node} 'cat {pipe_type} {dest}'"]
    else:
        if to_line is None:
            to_line = number_of_lines_in_file(src_node, src)
        cmd = ["ssh", src_node, f"sed -n '{from_line+1},{to_line}p' {src} | ssh {dest_node} 'cat {pipe_type} {dest}'"]

    print("Running", cmd)
    r = subprocess.run(cmd)

    if r.returncode == 0:
        return {
            "message": "Success"
        }
    else:
        return {
            "error": "Error during copy"
        }


def broadcast(args):
    """
    Broadcast files from source node to all destination nodes.

    :param args: user arguments
    """

    config = pd.read_csv(args.config)

    sources = config[config["direction"] == "source"]
    destinations = config[config["direction"] == "destination"]

    if len(sources) < 1:
        raise ValueError("There should be at least one source in the configuration.")

    if len(destinations) < 1:
        raise ValueError("There should be at least one destination in the configuration.")

    # every finished destination will become a source for the next iteration
    with tqdm(total=len(destinations), desc="Broadcasting") as pbar:
        while len(destinations) > 0:
            # run rsync via ssh in parallel
            futures = []
            with ThreadPoolExecutor() as executor:
                for i, (_, row) in enumerate(destinations[:len(sources)].iterrows()):  # take only as many destinations as sources
                    source_row = sources.iloc[i]
                    futures.append(
                        executor.submit(copy_rsync, source_row['node'], source_row['path'], row["node"], row["path"])
                    )

                for future in futures:
                    res = future.result()
                    if "error" in res:
                        raise RuntimeError(res["error"])
                pbar.update(len(futures))

            sources = pd.concat([sources, destinations[:len(futures)]])
            destinations = destinations.iloc[len(futures):]


def scatter_folder(sources: pd.DataFrame, destinations: pd.DataFrame):
    """
    Distribute folders from source node(s) to all destination nodes.

    :param sources: configuration of source nodes
    :param destinations: configuration of destination nodes
    """

    all_files = list_all_files_in_folder(sources.iloc[0]["node"], sources.iloc[0]["path"])
    if all_files is None:
        raise RuntimeError(f"Error during listing files in source folder on node {sources.iloc[0]['node']}")

    all_files = sorted(all_files)

    with tqdm(total=len(destinations), desc="Scattering folder") as pbar:
        for destinations_offset in range(0, len(destinations), len(sources)):
            # run rsync via ssh in parallel
            futures = []
            with ThreadPoolExecutor() as executor:
                for i, (_, row) in enumerate(destinations[destinations_offset:destinations_offset + len(sources)].iterrows()):
                    source_row = sources.iloc[i]
                    futures.append(
                        executor.submit(copy_rsync, source_row['node'], source_row['path'], row["node"],
                                        row["path"], all_files[int(row["from"]):int(row["to"])])
                    )

                for future in futures:
                    res = future.result()
                    if "error" in res:
                        raise RuntimeError(res["error"])
                pbar.update(len(futures))


def scatter_file(sources: pd.DataFrame, destinations: pd.DataFrame):
    """
    Distribute files from source node(s) to all destination nodes.

    :param sources: configuration of source nodes
    :param destinations: configuration of destination nodes
    """

    with tqdm(total=len(destinations), desc="Scattering file") as pbar:
        for _, row in destinations.iterrows():
            # run rsync via ssh in parallel
            futures = []
            with ThreadPoolExecutor() as executor:
                for _, source_row in sources.iterrows():
                    futures.append(
                        executor.submit(send_line_interval_from_file, source_row["node"], source_row["path"],
                                        row["node"], row["path"], int(row["from"]), int(row["to"]), True)
                    )

                for future in futures:
                    res = future.result()
                    if "error" in res:
                        raise RuntimeError(res["error"])
                pbar.update(len(futures))


def scatter(args):
    """
    Distribute files from source node to all destination nodes.

    :param args: user arguments
    """
    config = pd.read_csv(args.config)

    sources = config[config["direction"] == "source"]
    destinations = config[config["direction"] == "destination"]

    if len(sources) < 1:
        raise ValueError("There should be at least one source in the configuration.")

    if len(destinations) < 1:
        raise ValueError("There should be at least one destination in the configuration.")

    if is_file(sources.iloc[0]["node"], sources.iloc[0]["path"]):
        scatter_file(sources, destinations)
    else:
        scatter_folder(sources, destinations)


def gather_folder(source: pd.DataFrame, destinations: pd.DataFrame):
    """
    Gather folders from all destination nodes to source node.

    :param source: configuration of source node
    :param destinations: configuration of destination nodes
    """

    for _, row in tqdm(destinations.iterrows(), desc="Gathering folders", total=len(destinations)):
        # run rsync via ssh
        p = row["path"]
        if p[-1] != "/":
            p += "/"
        res = copy_rsync(row['node'], p, source.iloc[0]["node"], source.iloc[0]["path"])
        if "error" in res:
            raise RuntimeError(res["error"])


def gather_file(source: pd.DataFrame, destinations: pd.DataFrame):
    """
    Gather file parts from all destination nodes to source node.

    :param source: configuration of source node
    :param destinations: configuration of destination nodes
    """

    remove_file(source.iloc[0]["node"], source.iloc[0]["path"])

    for _, row in tqdm(destinations.iterrows(), desc="Gathering file", total=len(destinations)):
        # run rsync via ssh
        res = send_line_interval_from_file(row['node'], row['path'], source.iloc[0]["node"], source.iloc[0]["path"],
                                           append=True)
        if "error" in res:
            raise RuntimeError(res["error"])


def gather(args):
    """
    Gather files from all destination nodes to source node.

    :param args: user arguments
    """

    config = pd.read_csv(args.config)

    sources = config[config["direction"] == "source"]
    destinations = config[config["direction"] == "destination"]

    if len(sources) != 1:
        raise ValueError("There should be exactly one source in the configuration.")

    if len(destinations) < 1:
        raise ValueError("There should be at least one destination in the configuration.")

    if is_file(sources.iloc[0]["node"], sources.iloc[0]["path"]):
        gather_file(sources, destinations)
    else:
        gather_folder(sources, destinations)


def main():
    arg_parser = ArgumentParser(description="Script for distributed copying of folders and files.")

    subparsers = arg_parser.add_subparsers()

    broadcast_parser = subparsers.add_parser("broadcast", help="Broadcast files from source node to all destination nodes.")
    broadcast_parser.add_argument("config", help="Path to configuration of the broadcast.")
    broadcast_parser.set_defaults(func=broadcast)

    scatter_parser = subparsers.add_parser("scatter", help="Distribution of files from source node to all destination nodes.")
    scatter_parser.add_argument("config", help="Path to configuration of the distribute.")
    scatter_parser.set_defaults(func=scatter)

    gather_parser = subparsers.add_parser("gather", help="Gather files from all destination nodes to source node.")
    gather_parser.add_argument("config", help="Path to configuration of the gather.")
    gather_parser.set_defaults(func=gather)

    args = arg_parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
