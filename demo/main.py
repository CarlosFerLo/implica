import implica
from typing import List
import logging

# visualization
from graphviz import Source
import json
import tempfile
import webbrowser
from pathlib import Path


K = implica.Constant(
    "K",
    implica.TypeSchema("(A:*) -> (B:*) -> A"),
    lambda A, B: implica.BasicTerm("K", implica.Arrow(A, implica.Arrow(B, A))),
)
S = implica.Constant(
    "S",
    implica.TypeSchema("((A:*) -> (B:*) -> (C:*)) -> (A -> B) -> A -> C"),
    lambda A, B, C: implica.BasicTerm(
        "S",
        implica.Arrow(
            implica.Arrow(A, implica.Arrow(B, C)),
            implica.Arrow(implica.Arrow(A, B), implica.Arrow(A, C)),
        ),
    ),
)


class RunContext:
    objective: implica.TypeSchema

    type_vars: List[implica.Variable]

    def __init__(self, objective: implica.TypeSchema):
        logging.debug(f"Initializing RunContext with objective: {objective}")
        self.objective = objective
        self.type_vars = objective.get_type_vars()
        logging.debug(f"Extracted type variables: {self.type_vars}")


class Model:
    graph: implica.Graph

    max_iterations: int

    def __init__(self, constants: List[implica.Constant], max_iterations: int = 10):
        logging.debug(f"Initializing Model: {max_iterations} max iterations")
        self.graph = implica.Graph(constants=constants)
        self.max_iterations = max_iterations

    def run(self, query: str) -> str:

        objective = implica.TypeSchema(query)

        run_context = RunContext(objective)

        logging.debug(f"Adding objective node to graph: {objective}")
        self.graph.query().create(node="N", type_schema=objective).execute()

        for iteration in range(self.max_iterations):
            logging.debug(f"--- Iteration {iteration+1} ---")

            # Mark existing nodes
            self.graph.query().match("(N)").set("N", {"existed": True}, overwrite=True).execute()

            (
                self.graph.query()
                .match("(N:(B:*)->(A:*))")
                .where("N.existed")
                .merge("(M: A { existed: false })-[::@K(A, B)]->(N)")
                .execute()
            )
            (
                self.graph.query()
                .match("(N:(A:*))")
                .where("N.existed")
                .match("(M:(B:*))")
                .where("M.existed")
                .merge("(N)-[::@K(A, B)]->(:B->A { existed: false })")
                .execute()
            )
            (
                self.graph.query()
                .match("(N:((A:*)->(B:*))->A->(C:*))")
                .where("N.existed")
                .merge("(M:A->B->C { existed: false })-[::@S(A, B, C)]->(N)")
                .execute()
            )
            (
                self.graph.query()
                .match("(N:(A:*)->(B:*)->(C:*))")
                .where("N.existed")
                .merge("(N)-[::@S(A, B, C)]->(:(A->B)->A->C { existed: false })")
                .execute()
            )

            result = (
                self.graph.query().match(f"(:{run_context.objective.as_type()}:f)").return_("f")
            )

            if result:
                logging.debug(f"Objective achieved in iteration {iteration+1}")
                return str(result[0]["f"])

        logging.debug("Objective not achieved within max iterations")
        return "Objective not achieved within max iterations"

    def visualize(self) -> None:
        dot = self.graph.to_dot()
        Source(dot).render("model_graph", format="png", cleanup=True)

    def visualize_force_graph(self) -> None:
        json_data = json.loads(self.graph.to_force_graph_json())

        html = Path("./demo/viewer.html").read_text()
        html = html.replace("GRAPH_DATA", json.dumps(json_data))

        try:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
            tmp.write(html.encode("utf-8"))
            tmp.close()

            webbrowser.open(f"file://{tmp.name}")
        finally:
            tmp.delete = True


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(asctime)s - %(levelname)s] %(message)s",
    )

    model = Model(constants=[K, S], max_iterations=4)

    result = model.run("A -> A")
    print("Model run completed.")
    print(f"Result: {result}")
    print("Visualizing model graph...")
    model.visualize_force_graph()
