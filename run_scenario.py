"""Shortcut to run order simulator scenarios."""
import sys
import os

_script_dir = os.path.dirname(os.path.abspath(__file__))
_simulator_src = os.path.join(_script_dir, 'simulator', 'src')
sys.path.insert(0, _simulator_src)

from order_simulator.main import main

if __name__ == "__main__":
    main()