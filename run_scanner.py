#!/usr/bin/env python3
"""Entry point for SMA Crossover Scanner"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from crypto_sma_scanner.main import main

if __name__ == "__main__":
    main()
