import os
from typing import Optional, Dict, Sequence, Any

from vivarium.composites.toys import ToyCompartment
from vivarium.core.composition import test_composer
from vivarium.core.control import Control, run_library_cli
from vivarium.core.types import OutputDict
from vivarium.plots.simulation_output import plot_simulation_output


def toy_plot(
        data: OutputDict,
        config: Optional[Dict] = None,
        out_dir: Optional[str] = 'out'
) -> None:
    del config  # unused
    plot_simulation_output(data, out_dir=out_dir)


def toy_control(
        args: Optional[Sequence[str]] = None) -> Control:
    """ a toy example of control

    To run:
    > python vivarium/core/control.py -w 1
    """
    experiment_library = {
        # put in dictionary with name
        '1': {
            'name': 'exp_1',
            'experiment': test_composer},
        # map to function to run as is
        '2': test_composer,
    }
    plot_library = {
        # put in dictionary with config
        '1': {
            'plot': toy_plot,
            'config': {}},
        # map to function to run as is
        '2': toy_plot
    }
    composers_library = {
        'agent': ToyCompartment,
    }
    workflow_library = {
        '1': {
            'name': 'test_workflow',
            'experiment': '1',
            'plots': ['1']},
        '2': {
            'name': 'test_workflow',
            'experiment': '1',
            'plots': '2'}
    }

    control = Control(
        out_dir=os.path.join('out', 'control_test'),
        experiments=experiment_library,
        composers=composers_library,
        plots=plot_library,
        workflows=workflow_library,
        args=args,
    )

    return control


def test_library_cli() -> None:
    def run_fun(key: Any = False) -> dict:
        return {'key': key}
    lib = {'1': run_fun}
    run_library_cli(lib, args=['-n', '1', '-o', 'key=True'])
    run_library_cli(lib, args=['-n', '1', '-o', 'key=0.2'])
    run_library_cli(lib, args=['-n', '1', '-o', 'key=b'])


def test_control() -> None:
    toy_control(args=['-w', '1'])
    toy_control(args=['-w', '2'])
    control = toy_control(args=['-e', '2'])
    control.run_workflow('1')


fun_lib = {
    '0': test_library_cli,
    '1': test_control,
}



# python vivarium/experiments/control_tests.py -n [test number]
if __name__ == '__main__':
    run_library_cli(fun_lib)
