import argparse
from pathlib import Path
from simulator import Simulator

def default_settings():
    base_folder = Path(__file__).resolve().parent.parent
    settings_file = base_folder / 'config/settings.json'
    return settings_file

def is_valid_file(parser, arg):
    settings_file = Path(arg)
    if not settings_file.is_file():
        return parser.error(f"argument -f/--file: can't open '{arg}'")
    return settings_file

parser = argparse.ArgumentParser(description='MQTT Simulator for publishing and subscribing')
parser.add_argument('-f', '--file', dest='settings_file', type=lambda x: is_valid_file(parser, x), 
                    help='settings file', default=default_settings())
parser.add_argument('-m', '--mode', choices=['pub', 'sub', 'both'], default='both',
                    help='Run mode: publisher only, subscriber only, or both (default)')
parser.add_argument('-o', '--output', type=str, default='./data',
                    help='Directory to store collected subscription data')
args = parser.parse_args()

simulator = Simulator(args.settings_file)
simulator.run()