import squid


def simulation(agent):
    fitness = 42
    data = {"data": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]}

    return fitness, data


if __name__ == "__main__":
    squid.run(simulation)
