import implica
from typing import List
import logging


K = implica.Constant(
    "K", lambda A, B: implica.BasicTerm("K", implica.Arrow(A, implica.Arrow(B, A)))
)
S = implica.Constant(
    "S",
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

    def run(self, query: str):

        objective = implica.TypeSchema(query)

        run_context = RunContext(objective)

        logging.debug(f"Adding objective node to graph: {objective}")
        self.graph.query().create(node="N", type_schema=objective).execute()

        for iteration in range(self.max_iterations):
            logging.debug(f"--- Iteration {iteration} ---")

            # Mark existing nodes
            self.graph.query().match("(N)").set("N", {"existed": True}, overwrite=True).execute()

            self.graph.query().match("(N:(B:*)->(A:*))").where("N.existed").merge(
                "(M: A { existed: false })"
            ).merge("(M)-[::@K(A, B)]->(N)").execute()
            self.graph.query().match("(N:(A:*))").where("N.existed").match("(M:(B:*))").where(
                "M.existed"
            ).merge("(N)-[::@K(A, B)]->(:B->A { existed: false })").execute()

            result = self.graph.query().match("(N)-[E]->(M)").return_("N", "E", "M")

            for record in result:
                logging.debug(f"Record: N={record['N']}, E={record['E']}, M={record['M']}")


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s - %(levelname)s] %(message)s")

    model = Model(constants=[K, S], max_iterations=10)

    model.run("A -> A")
