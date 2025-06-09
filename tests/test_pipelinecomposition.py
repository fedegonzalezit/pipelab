import unittest
from pipelab.pipeline import Pipeline, PipelineComposition, PipelineStep


class DummyStep(PipelineStep):
    def execute(self, pipeline: Pipeline, **kwargs):
        # Save a marker artifact to check execution order
        pipeline.save_artifact(self.name, f"executed_{self.name}")
        return {self.name: f"executed_{self.name}"}


class TestPipelineComposition(unittest.TestCase):
    def setUp(self):
        # Create pipelines and steps
        self.p1 = Pipeline(optimize_arftifacts_memory=False)
        self.p2 = Pipeline(optimize_arftifacts_memory=False)
        self.p3 = Pipeline(optimize_arftifacts_memory=False)
        self.s1 = DummyStep(name="step1")
        self.s2 = DummyStep(name="step2")
        self.s3 = DummyStep(name="step3")
        self.p1.add_step(self.s1)
        self.p2.add_step(self.s2)
        self.p3.add_step(self.s3)
        # p1 -> [p2, p3], p2 -> [p3]
        self.composition = PipelineComposition(
            {self.p1: [self.p2, self.p3], self.p2: [self.p3], self.p3: []}
        )

    def test_parents_set_correctly(self):
        # p2 and p3 should have p1 as parent, p3 should also have p2 as parent
        self.assertIn(self.p1, self.p2.parents)
        self.assertIn(self.p1, self.p3.parents)
        self.assertIn(self.p2, self.p3.parents)
        self.assertEqual(self.p1.parents, [])

    def test_topological_order(self):
        order = self.composition._topological_sort()
        # p1 must be before p2 and p3, p2 before p3
        self.assertLess(order.index(self.p1), order.index(self.p2))
        self.assertLess(order.index(self.p1), order.index(self.p3))
        self.assertLess(order.index(self.p2), order.index(self.p3))

    def test_run_executes_in_order(self):
        self.composition.run()
        # All pipelines should have their step artifact
        self.assertEqual(self.p1.get_artifact("step1"), "executed_step1")
        self.assertEqual(self.p2.get_artifact("step2"), "executed_step2")
        self.assertEqual(self.p3.get_artifact("step3"), "executed_step3")


if __name__ == "__main__":
    unittest.main()
