import os
import time
import gc
import pickle
import inspect
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod
from pipelab.cache import CachedPipelineMixin


class ArtifactNotFoundError(Exception):
    """Custom exception for when an artifact is not found in the pipeline."""

    def __init__(self, artifact_name: str):
        super().__init__(f"Artifact '{artifact_name}' not found in the pipeline.")
        self.artifact_name = artifact_name


class PipelineStep(ABC, CachedPipelineMixin):
    """
    Abstract base class for pipeline steps.
    Each step in the pipeline must inherit from this class and implement the execute method.
    """

    def __init__(self, name: Optional[str] = None):
        """
        Initialize a pipeline step.

        Args:
            name (str): Name of the step for identification and logging purposes.
        """
        self._name = name or self.__class__.__name__

    @abstractmethod
    def execute(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Execute the pipeline step.

        Args:
            pipeline (Pipeline): The pipeline instance that contains this step.
        """
        pass

    def execute_inverse(self, pipeline, **kwargs: Any) -> Dict[str, Any]:
        """
        Execute the inverse operation of this step.
        This is useful for undoing transformations or reverting changes made by the step.

        Args:
            pipeline (Pipeline): The pipeline instance that contains this step.
        """
        return {}

    def save_artifact(
        self, pipeline: "Pipeline", artifact_name: str, artifact: Any
    ) -> None:
        """
        Save an artifact produced by this step to the pipeline.

        Args:
            pipeline (Pipeline): The pipeline instance.
            artifact_name (str): Name to identify the artifact.
            artifact (Any): The artifact to save.
        """
        pipeline.save_artifact(artifact_name, artifact)

    def get_artifact(
        self,
        pipeline: "Pipeline",
        artifact_name: str,
        default=None,
        raise_not_found=True,
    ) -> Any:
        """
        Retrieve a stored artifact from the pipeline.

        Args:
            pipeline (Pipeline): The pipeline instance.
            artifact_name (str): Name of the artifact to retrieve.
            default: Default value to return if the artifact is not found.
            raise_not_found (bool): Whether to raise an error if the artifact is not found.

        Returns:
            Any: The requested artifact or default value.
        """
        return pipeline.get_artifact(
            artifact_name, default=default, raise_not_found=raise_not_found
        )

    def del_artifact(self, pipeline: "Pipeline", artifact_name: str, soft=True) -> None:
        """
        Delete a stored artifact from the pipeline and free memory.

        Args:
            pipeline (Pipeline): The pipeline instance.
            artifact_name (str): Name of the artifact to delete.
            soft (bool): If True, performs a soft delete; if False, forces garbage collection.
        """
        pipeline.del_artifact(artifact_name, soft=soft)

    def get_execute_params(self):
        sig = inspect.signature(self.execute)
        return sig.parameters

    def get_execute_inverse_params(self):
        sig = inspect.signature(self.execute_inverse)
        return sig.parameters

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value


class ArtifactManager(ABC):
    """
    Abstract base class for managing artifacts in a pipeline.
    This class defines the interface for saving, retrieving, and deleting artifacts.
    """

    @abstractmethod
    def save_artifact(self, artifact_name: str, artifact: Any) -> None:
        """Save an artifact with a given name."""
        pass

    @abstractmethod
    def get_artifact(
        self, artifact_name: str, default=None, raise_not_found=True
    ) -> Any:
        """Retrieve an artifact by its name."""
        pass

    @abstractmethod
    def del_artifact(self, artifact_name: str, soft=True) -> None:
        """Delete an artifact by its name."""
        pass


class ArtifactInMemory(ArtifactManager):
    """
    In-memory artifact manager that stores artifacts in a dictionary.
    This is useful for small artifacts that can fit in memory.
    """

    def __init__(self, *args, **kwargs):
        self.artifacts: Dict[str, Any] = {}

    def save_artifact(self, artifact_name: str, artifact: Any) -> None:
        self.artifacts[artifact_name] = artifact

    def get_artifact(
        self, artifact_name: str, default=None, raise_not_found=True
    ) -> Any:
        if raise_not_found and artifact_name not in self.artifacts:
            raise ArtifactNotFoundError(artifact_name)
        return self.artifacts.get(artifact_name, default)

    def del_artifact(self, artifact_name: str) -> None:
        if artifact_name in self.artifacts:
            del self.artifacts[artifact_name]

    def clear(self) -> None:
        """
        Clear all stored artifacts and free memory.
        """
        self.artifacts.clear()


class ArtifactInDisk(ArtifactManager):
    """
    Disk-based artifact manager that stores artifacts in files.
    This is useful for larger artifacts that should not be kept in memory.
    """

    def __init__(self, pipeline_name, directory: str = "/tmp/"):
        self.directory = os.path.join(directory, pipeline_name)
        self.artifacts = {}
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

    def save_artifact(self, artifact_name: str, artifact: Any) -> None:
        artifact_path = os.path.join(self.directory, artifact_name)
        with open(artifact_path, "wb") as f:
            pickle.dump(artifact, f)
        self.artifacts[artifact_name] = artifact_path

    def get_artifact(
        self, artifact_name: str, default=None, raise_not_found=True
    ) -> Any:
        artifact_path = self.artifacts.get(
            artifact_name, os.path.join(self.directory, artifact_name)
        )
        if raise_not_found and not os.path.exists(artifact_path):
            raise ArtifactNotFoundError(artifact_name)
        elif not os.path.exists(artifact_path):
            return default
        with open(artifact_path, "rb") as f:
            return pickle.load(f)

    def del_artifact(self, artifact_name: str) -> None:
        artifact_path = self.artifacts.get(
            artifact_name, os.path.join(self.directory, artifact_name)
        )
        if os.path.exists(artifact_path):
            os.remove(artifact_path)

    def clear(self) -> None:
        """
        Clear all stored artifacts and free memory.
        """
        for artifact_name in list(self.artifacts.keys()):
            self.del_artifact(artifact_name)
        self.artifacts.clear()
        if os.path.exists(self.directory):
            os.rmdir(self.directory)


class Pipeline:
    """
    Main pipeline class that manages the execution of steps and storage of artifacts.
    """

    def __init__(
        self,
        name="default_pipeline",
        steps: Optional[List[PipelineStep]] = None,
        optimize_arftifacts_memory: bool = True,
    ):
        """Initialize the pipeline."""
        self.name = name
        self.steps: List[PipelineStep] = steps if steps is not None else []
        self.artifact_manager = (
            ArtifactInDisk(self.name)
            if optimize_arftifacts_memory
            else ArtifactInMemory()
        )
        self.finished = False
        self._parents = []
        self._processed_stack: List[PipelineStep] = []

    def add_parent(self, parent: "Pipeline") -> None:
        """
        Add a parent pipeline to this pipeline.

        Args:
            parent (Pipeline): The parent pipeline to add.
        """
        if parent not in self._parents:
            self._parents.append(parent)

    @property
    def parents(self) -> List["Pipeline"]:
        """
        Get the list of parent pipelines.

        Returns:
            List[Pipeline]: List of parent pipelines.
        """
        return self._parents

    def add_step(self, step: PipelineStep, position: Optional[int] = None) -> None:
        """
        Add a new step to the pipeline.

        Args:
            step (PipelineStep): The step to add.
            position (Optional[int]): Position where to insert the step. If None, appends to the end.
        """
        if position is not None:
            self.steps.insert(position, step)
        else:
            self.steps.append(step)

    def save_artifact(self, artifact_name: str, artifact: Any) -> None:
        """
        Save an artifact from a given step.

        Args:
            artifact_name (str): Name to identify the artifact.
            artifact (Any): The artifact to save.
        """
        self.artifact_manager.save_artifact(artifact_name, artifact)

    def get_artifact(
        self, artifact_name: str, default=None, raise_not_found=True
    ) -> Any:
        """
        Retrieve a stored artifact.

        Args:
            artifact_name (str): Name of the artifact to retrieve.

        Returns:
            Any: The requested artifact.
        """
        try:
            return self.artifact_manager.get_artifact(
                artifact_name, default=default, raise_not_found=raise_not_found
            )
        except ArtifactNotFoundError as e:
            # look for the artifact in parent pipelines
            for parent in self.parents:
                try:
                    return parent.get_artifact(
                        artifact_name, default=default, raise_not_found=raise_not_found
                    )
                except ArtifactNotFoundError:
                    continue
            if raise_not_found:
                raise ArtifactNotFoundError(artifact_name) from e
        return default

    def del_artifact(self, artifact_name: str) -> None:
        """
        Delete a stored artifact and free memory.

        Args:
            artifact_name (str): Name of the artifact to delete.
        """
        self.artifact_manager.del_artifact(artifact_name)

    def run(self, verbose: bool = True) -> None:
        """
        Execute all steps in sequence and log execution time.
        """

        # Run steps from the last completed step
        if self.finished:
            if verbose:
                print("Pipeline has already finished. Skipping execution.")
            return

        for step in self.steps:
            if verbose:
                print(f"Executing step: {step.name}")
            start_time = time.time()
            params = self.__fill_params_from_step(step)
            artifacts_to_save = step.execute(**params)
            if artifacts_to_save is None:
                artifacts_to_save = {}
            self.__save_step_artifacts(artifacts_to_save)
            end_time = time.time()
            if verbose:
                print(
                    f"Step {step.name} completed in {end_time - start_time:.2f} seconds"
                )
            self._processed_stack.append(step)
        self.finished = True

    def __fill_params_from_step(self, step) -> Dict[str, Any]:
        """
        Obtiene los nombres de los parametros de la implementacion de la funcion execute del paso. (excepto el pipeline el cual es obligatorio)
        luego obtengo todos los artefactos del pipeline y los paso como parametros al paso.
        """
        step_params = step.get_execute_params()
        params = {}
        for name, param in step_params.items():
            if param.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                continue  # Skip *args and **kwargs
            if name == "pipeline":
                params[name] = self
            elif param.default is inspect.Parameter.empty:
                params[name] = self.get_artifact(name)
            else:
                params[name] = self.get_artifact(
                    name, default=param.default, raise_not_found=False
                )
        return params

    def __save_step_artifacts(self, artifacts_to_save: Dict[str, Any]) -> None:
        """
        Save artifacts produced by a step to the pipeline.

        Args:
            artifacts_to_save (Dict[str, Any]): Artifacts to save.
        """

        for name, artifact in artifacts_to_save.items():
            self.save_artifact(name, artifact)

    def reverse_steps(self, **kwargs):
        """
        Recorre el stack de pasos procesados y ejecuta el metodo execute_inverse de cada paso.
        El objetivo de este metodo es realizar transformaciones inversas de los targets para evaluacion de resultados.
        """
        last_response = kwargs
        for step in reversed(self._processed_stack):
            last_response = step.execute_inverse(self, **last_response)
        return last_response

    def clear(self, collect_garbage: bool = False) -> None:
        """
        Clean up all artifacts and free memory.
        """
        self.artifact_manager.clear()
        if collect_garbage:
            gc.collect()
        self.finished = False


class PipelineComposition:
    """
    self.pipelines es un grafo de pipelines, donde cada pipeline puede tener varios pipelines hijos.
    """

    def __init__(self, pipelines: Dict[Pipeline, List[Pipeline]]):
        self.pipelines = pipelines
        self._set_parents()

    def _set_parents(self):
        # Limpia los padres actuales
        for pipeline in self.pipelines:
            pipeline._parents = []
        # Asigna padres a cada hijo
        for parent, children in self.pipelines.items():
            for child in children:
                child.add_parent(parent)

    def _topological_sort(self):
        visited = set()
        order = []

        def visit(pipeline):
            if pipeline in visited:
                return
            visited.add(pipeline)
            for child in self.pipelines.get(pipeline, []):
                visit(child)
            order.append(pipeline)

        # Ejecuta DFS desde todos los nodos raíz (sin padres)
        all_pipelines = set(self.pipelines.keys()) | {
            c for children in self.pipelines.values() for c in children
        }
        roots = [p for p in all_pipelines if not getattr(p, "parents", [])]
        for root in roots:
            visit(root)
        return order[::-1]  # De padres a hijos

    def run(self):
        order = self._topological_sort()
        for pipeline in order:
            pipeline.run()
