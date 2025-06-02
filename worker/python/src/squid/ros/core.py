from abc import ABC, abstractmethod
from typing import Type
from multiprocessing import Process

import xml.etree.ElementTree as ET
from squid.util import *
from squid.core import _supervisor, _worker_proc
import rclpy
from rclpy.node import Node
from rclpy.parameter import Parameter


class AgentNode(Node, ABC):
    """
    An abstract class that inherits from the rclpy `Node` class.

    The index of the process is provided via `self.squid_ix`, and should be used to
    identify which model to control.

    This class includes one abstract method, `sim`, whose implementation is responsible for
    simulating and evaluating the agent.

    This class also sets the `use_sim_time` ROS parameter to true.
    """

    def __init__(self, **kwargs):
        node_name = kwargs.pop("node_name")
        self.squid_ix = kwargs.pop("squid_ix")
        super().__init__(node_name, **kwargs)
        self.set_parameters([Parameter("use_sim_time", Parameter.Type.BOOL, True)])

    @abstractmethod
    def sim(self, genome: str) -> tuple[float, dict[str, list[list[float]]] | None]:
        """
        Takes in a JSON string `genome` and returns a fitness score and an optional dict with additional experimental data.
        This method should:
        - deserialize the JSON string into an instance of the neural network
        - respawn the sdf model
        - control the vehicle via neural network outputs
        - collect data for evaluation
        - evaluate the fitness function
        - return the fitness and any additional data
        """
        ...


def _make_sdf_and_name(sdf: str, i: int):
    sdf_tree = ET.fromstring(sdf)
    model_elem = sdf_tree.find("model")
    if model_elem is None:
        raise ValueError("No <model> element found in the SDF string.")
    model_name = f'{model_elem.get("name")}_{i}'
    model_elem.set("name", model_name)

    collide_bitmask_val = hex(1 << i)

    for collision in model_elem.findall(".//collision"):
        surface = collision.find("surface")
        if surface is None:
            surface = ET.SubElement(collision, "surface")
        contact = surface.find("contact")
        if contact is None:
            contact = ET.SubElement(surface, "contact")
        collide_bitmask = contact.find("collide_bitmask")
        if collide_bitmask is None:
            collide_bitmask = ET.SubElement(contact, "collide_bitmask")
        collide_bitmask.text = collide_bitmask_val

    return ET.tostring(sdf_tree, encoding="unicode"), model_name


def _ros_worker_proc(
    Node: Type[AgentNode],
    i: int,
    wk_id: int,
    exp_id_b: bytes,
    wk_router_url: str,
):
    rclpy.init()
    node = Node(
        node_name=f"squid_rosagent_{i}",
        squid_ix=i,
    )
    _worker_proc(node.sim, wk_id, exp_id_b, wk_router_url)
    node.destroy_node()
    rclpy.shutdown()


def ros_worker(node_cls: Type[AgentNode]):
    """
    Run the Squid ROS node worker.

    This function must be called in the entrypoint of the simulation docker image.

    :param node_cls: A class that inherits from `AgentNode` with a `sim` implementation.
    """

    def proc_factory(exp_id_b, wk_router_url, wk_ids):
        workers = {}
        for i, wk_id in enumerate(wk_ids):
            p = Process(
                target=_ros_worker_proc,
                args=(
                    node_cls,
                    i,
                    wk_id,
                    exp_id_b,
                    wk_router_url,
                ),
            )
            workers[wk_id] = p
            p.start()
        return workers

    _supervisor(proc_factory)
