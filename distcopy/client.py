# -*- coding: UTF-8 -*-
from typing import Optional

from classconfig import ConfigurableValue


class Client:

    token: Optional[str] = ConfigurableValue("Token for authentication", voluntary=True)

    def __init__(self, token: Optional[str] = None):
        """
        Initialize client with token for authentication.

        :param token: token for authentication
        """
        self.token = token