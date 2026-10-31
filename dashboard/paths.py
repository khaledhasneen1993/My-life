"""Three isolated sources and state files; explicit paths override preserved accounts."""
import hashlib
import os
from pathlib import Path

ASSETS = Path(__file__).resolve().parent
ROOT = ASSETS.parent
RUNTIME = Path(os.environ.get('CASCADE_RUNTIME_DIR', str(ROOT / '.runtime/dashboard'))).expanduser().resolve()
SOURCES = {
    'final': Path(os.environ.get('CASCADE_FINAL_DIR', str(ROOT / '.runtime/repos/Final'))).expanduser().resolve(),
    'final3': Path(os.environ.get('CASCADE_FINAL3_DIR', str(ROOT.parent / 'Final-3'))).expanduser().resolve(),
    'mylife': ROOT,
}
NAMES = ('final', 'final3', 'mylife')
LABELS = {'final': 'Final', 'final3': 'Final-3', 'mylife': 'My-life'}


def previous_final_state():
    record = SOURCES['final3'] / '.runtime/account-location'
    if record.is_file():
        previous = Path(record.read_text().strip()).expanduser()
        if previous.is_absolute():
            return previous / 'data/final.state.json'
    previous = ROOT.parent / 'cascade-termux/data/final.state.json'
    return previous if previous.is_file() else RUNTIME / 'data/final.state.json'


STATE_PATHS = {
    'final': Path(os.environ.get('CASCADE_FINAL_STATE', str(previous_final_state()))).expanduser().resolve(),
    'final3': Path(os.environ.get('CASCADE_FINAL3_STATE', str(SOURCES['final3'] / 'paper_state_final3.json'))).expanduser().resolve(),
    'mylife': Path(os.environ.get('CASCADE_MYLIFE_STATE', str(ROOT / 'paper_state_final3_pro.json'))).expanduser().resolve(),
}
if len(set(STATE_PATHS.values())) != len(NAMES):
    raise ValueError('Each bot must use its own state file')


def archive_path(name):
    key = hashlib.sha256(str(STATE_PATHS[name]).encode()).hexdigest()[:16]
    return RUNTIME / 'data' / f'{name}-{key}.fills.sqlite'
