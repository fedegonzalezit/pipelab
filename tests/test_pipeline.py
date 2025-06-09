import unittest
import os
from pipelab.pipeline import Pipeline, PipelineStep, ArtifactNotFoundError
import io
import sys


class DummyStep(PipelineStep):
    def execute(self, pipeline: Pipeline, x=None):
        return {"result": x if x is not None else 42}


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.pipeline = Pipeline(name="TestPipeline", optimize_arftifacts_memory=False)
        self.step = DummyStep()
        self.pipeline.add_step(self.step)

    def test_add_and_run_step(self):
        self.pipeline.run(verbose=False)
        self.assertIn("result", self.pipeline.artifact_manager.artifacts)
        self.assertEqual(self.pipeline.get_artifact("result"), 42)

    def test_save_and_get_artifact(self):
        self.pipeline.save_artifact("foo", 123)
        self.assertEqual(self.pipeline.get_artifact("foo"), 123)

    def test_del_artifact(self):
        self.pipeline.save_artifact("bar", 456)
        self.pipeline.del_artifact("bar")
        self.assertNotIn("bar", self.pipeline.artifact_manager.artifacts)

    def test_clear(self):
        self.pipeline.save_artifact("baz", 789)
        self.pipeline.clear()
        self.assertEqual(self.pipeline.artifact_manager.artifacts, {})
        self.assertFalse(self.pipeline.finished)

    def test_optimize_artifacts_memory(self):
        pipeline = Pipeline(name="Test Pipeline", optimize_arftifacts_memory=True)
        pipeline.save_artifact("tmp_artifact", {"a": 1})
        path = pipeline.artifact_manager.artifacts["tmp_artifact"]
        self.assertTrue(os.path.exists(path))
        loaded = pipeline.get_artifact("tmp_artifact")
        self.assertEqual(loaded, {"a": 1})
        pipeline.del_artifact("tmp_artifact")
        if os.path.exists(path):
            os.remove(path)

    def test_get_artifact_not_found(self):
        pipeline = Pipeline(name="Test Pipeline", optimize_arftifacts_memory=True)
        # Not saving artifact, should raise FileNotFoundError
        with self.assertRaises(ArtifactNotFoundError):
            pipeline.get_artifact("not_exist")
        # Should return default if raise_not_found=False
        self.assertEqual(
            pipeline.get_artifact("not_exist", default=123, raise_not_found=False), 123
        )

    def test_clear_collect_garbage(self):
        pipeline = Pipeline(optimize_arftifacts_memory=False)
        pipeline.save_artifact("foo", 1)
        pipeline.finished = True
        pipeline.clear(collect_garbage=True)
        self.assertEqual(pipeline.artifact_manager.artifacts, {})
        self.assertFalse(pipeline.finished)

    def test_run_already_finished(self):
        pipeline = Pipeline(optimize_arftifacts_memory=False)
        pipeline.finished = True

        captured = io.StringIO()
        sys_stdout = sys.stdout
        sys.stdout = captured
        try:
            pipeline.run(verbose=True)
        finally:
            sys.stdout = sys_stdout
        self.assertIn("Pipeline has already finished", captured.getvalue())

    def test_fill_params_from_step_required_optional(self):
        class Step(PipelineStep):
            def execute(self, pipeline, foo, bar=2):
                return {"foo": foo, "bar": bar}

        pipeline = Pipeline(optimize_arftifacts_memory=False)
        pipeline.save_artifact("foo", 10)
        pipeline.save_artifact("bar", 20)
        step = Step()
        params = pipeline._Pipeline__fill_params_from_step(step)
        self.assertEqual(params["foo"], 10)
        self.assertEqual(params["bar"], 20)
        self.assertIs(params["pipeline"], pipeline)


if __name__ == "__main__":
    unittest.main()
