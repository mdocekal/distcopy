# distcopy
Script for distributed copying of folders and files.

# Broadcast

It allows to distribute files and folders to multiple destinations from one or multiple sources. It expects csv files defining those sources and destinations.

It can be run with the following command:

```bash
./run.py broadcast config.csv
```

An example of a csv file is:

```csv
direction,node,path
source,athena1,/path/to/source
destination,athena2,/path/to/destination
destination,athena3,/path/to/destination
destination,athena4,/path/to/destination
destination,athena5,/path/to/destination
destination,athena6,/path/to/destination
destination,athena7,/path/to/destination
destination,athena8,/path/to/destination
```

The script will firstly make a `rsync` from the source node `athena1` to the first destination `athena2`. In the second
iteration it will reuse the already copied files from `athena2` to copy them to `athena3` and `athena4` from `athena1` and `athena2`. 
Then it will again reuse all the existing copies to copy them to `athena5`, `athena6`, `athena7` and `athena8`.

You can also specify multiple sources if you already have multiple copies of the same data.

# Scatter
This allows to distribute subparts on multiple nodes.

It can be run with the following command:

```bash
./run.py scatter config.csv
```

An example of a csv file is:

```csv
direction,node,path,from,to
source,athena1,/path/to/source,,
destination,athena2,/path/to/destination,0,2
destination,athena3,/path/to/destination,2,4
destination,athena4,/path/to/destination,4,6
destination,athena5,/path/to/destination,6,8
destination,athena6,/path/to/destination,8,10
destination,athena7,/path/to/destination,10,12
destination,athena8,/path/to/destination,12,14
```

The `from` and `to` columns are specifying the range `[from, to)` of the subpart of the source that should be copied to the destination. These fields are ignored for the source nodes. It is possible to use multiple source nodes to improve the load balancing.

When the source is file the `from` and `to` fields are line offsets. When the source is a folder all files are sorted according to path alphabetically and `from` and `to` are offsets to the sorted list.

# Gather
This allows to gather subparts from multiple nodes.

It can be run with the following command:

```bash
./run.py gather config.csv
```

An example of a csv file is:

```csv
direction,node,path
source,athena1,/path/to/source
destination,athena2,/path/to/destination
destination,athena3,/path/to/destination
destination,athena4,/path/to/destination
destination,athena5,/path/to/destination
destination,athena6,/path/to/destination
destination,athena7,/path/to/destination
destination,athena8,/path/to/destination
```

Now the logic of direction is different, to allow to reuse the scatter config as much as possible. Now the source is 
the node where the data is gathered and the destination nodes are the nodes where the data are gathered from.

When split file is gathered the order of the line parts is given by the order of the destination nodes in the csv file.

# Path backslashes

The trailing backslashes act the same as in rsync. 

If the source path ends with a backslash, the content of the folder is copied. If the source path does not end with a backslash, the folder itself is copied.

# Connection note
The script uses `rsync` to copy the files. It is expected that the nodes are accessible via ssh without password.