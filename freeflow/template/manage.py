#!/usr/bin/env python
"""Command line utility for development and deployment"""
try:
  from freeflow.core.cli import execute
except ImportError as e:
  print(e)
  raise ImportError(
    "Couldn't find Freeflow. Are you sure it's installed?"
  )

## TO-DO Core capability:
# - deploying: locally and composer (setup vars, conn, etc)
# - running the thing? running the thing!

def main(argv=None):
  execute(argv)

if __name__ == '__main__':
  main()
