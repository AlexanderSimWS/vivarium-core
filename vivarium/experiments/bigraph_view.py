from vivarium.core.engine import Engine, pf
from vivarium.core.control import run_library_cli
from vivarium.experiments.engine_tests import get_toy_transport_in_env_composite
from vivarium.core.process import Step
from vivarium.core.types import (
    Schema, Update, Union, State, Flow, Topology)
from vivarium.processes.meta_division import daughter_phylogeny_id


class TopView(Step):

    def ports_schema(self) -> Schema:
        return {
            'top': '**',
            'other': {
                '_default': 2.0
            }
        }

    def next_update(
            self, timestep: Union[float, int], states: State) -> Update:
        assert states['other'] == 2.0, 'not getting access to other state'
        top = states['top']
        agents = top['agents']

        # update the bigraph directly
        update: Update = {'top': {'agents': {}}}
        for agent_id, agent_state in agents.items():
            internal_glc = agent_state['internal']['GLC']
            if internal_glc >= 6.0:
                # trigger division
                daughter_ids = daughter_phylogeny_id(agent_id)
                daughter_updates = [
                    {'key': daughter_id}
                    for daughter_id in daughter_ids]
                update['top']['agents'] = {
                    '_divide': {
                        'mother': agent_id,
                        'daughters': daughter_updates}}
        return update


def test_bigraph_view() -> None:
    agent_id = '1'

    top_view_steps = {'top_view': TopView()}
    top_view_flow: Flow = {'top_view': []}
    top_view_topology: Topology = {
        'top_view': {
            'top': (),  # connect to the top
            'other': ('other',),
        }
    }

    composite = get_toy_transport_in_env_composite(agent_id=agent_id)
    composite.merge(
        steps=top_view_steps,
        topology=top_view_topology,
        flow=top_view_flow)

    # run the simulation
    sim = Engine(
        composite=composite,
        initial_state={
            'agents': {agent_id: {'external': {'GLC': 10.0}}}}
    )
    sim.update(20)
    data = sim.emitter.get_data()

    print(pf(data))
    len(data[20.0]['agents'])


scans_library = {
    '0': test_bigraph_view,
}

# python vivarium/experiments/bigraph_view.py -n [name]
if __name__ == '__main__':
    run_library_cli(scans_library)
