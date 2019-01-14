"""@package exceptions
Elo exceptions

This script contains all Elo frontend specific exceptions.

@file exceptions.py

@author Tyler Shake

@par Notifications:

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The below copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

@copyright Copyright 2019 Tyler Shake

"""

class Error(Exception):
    """Base class for exceptions"""
    pass

class ConfigError(Error):
    """An config file or configuration exception.

    Args:
        msg (str):  The error message

    Attributes:
        msg (str):  The error message

    """

    def __init__(self, msg):
        """Constructor with error message argument."""

        super(ConfigError, self).__init__(msg)
        self.msg = msg

class HTTPError(Error):
    """An HTTP exception.

    Args:
        msg (str):  The error message

    Attributes:
        msg (str):  The error message

    """

    def __init__(self, msg):
        """Constructor with error message argument."""

        super(HTTPError, self).__init__(msg)
        self.msg = msg

class DBValueError(Error):
    """An Database value exception.

    Args:
        msg (str):  The error message

    Attributes:
        msg (str):  The error message

    """

    def __init__(self, msg):
        """Constructor with error message argument."""

        super(DBValueError, self).__init__(msg)
        self.msg = msg

class DBConnectionError(Error):
    """An Database connection exception.

    Args:
        msg (str):  The error message

    Attributes:
        msg (str):  The error message

    """

    def __init__(self, msg):
        """Constructor with error message argument."""

        super(DBConnectionError, self).__init__(msg)
        self.msg = msg

class DBSyntaxError(Error):
    """An Database programming syntax exception.

    Args:
        msg (str):  The error message

    Attributes:
        msg (str):  The error message

    """

    def __init__(self, msg):
        """Constructor with error message argument."""

        super(DBSyntaxError, self).__init__(msg)
        self.msg = msg
