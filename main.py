# main.py
import graph_utils
import agent_help_scouts



def main():
    # ─── demo topology ───
    nodes  = 10
    agent_count = 10

    G = graph_utils.create_port_labeled_graph(nodes, 4, 42)
    agents = [agent_help_scouts.Agent(i, 0) for i in range(agent_count)]

    # ─── run ───
    agent_help_scouts.run_simulation(G, agents, 0, 0, {})

    print("\nAgent final states:")
    for a in agents:
        state = a.state
        print(f"  A{a.ID} @ node {a.node:>2}  →  {state}")
        print(f"  A{a.ID} @ node {a.home:>2}  →  {state}")
        print()


if __name__ == "__main__":
    main()
