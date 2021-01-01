from abc import abstractmethod
from ..transform import Transform, TransformMeta
from ..util import classproperty
import copyreg


class CombinatorMeta(TransformMeta):
    @staticmethod
    def reduce(cls):
        return (cls.__creation_values__[0], cls.__creation_values__[1:])


copyreg.pickle(CombinatorMeta, CombinatorMeta.reduce)


class CombinatorBase(Transform, metaclass=CombinatorMeta):
    @property
    @abstractmethod
    def substeps(self):
        pass

    @classproperty
    def parallelism(cls):
        max_parallelism = 0
        for step in cls.substeps:
            if step.parallelism > max_parallelism:
                max_parallelism = step.parallelism
        return max_parallelism

    # TODO: Clean this up by propagating touches to higher levels
    def _warn_if_files_untouched(self):
        pass
