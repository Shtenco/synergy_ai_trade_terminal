#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Legacy entrypoint.

Перенаправляет на новый Cassandra-ориентированный модуль:
`cassandra_deepseek_supervisor.py`.
"""

from cassandra_deepseek_supervisor import main


if __name__ == "__main__":
    main()
