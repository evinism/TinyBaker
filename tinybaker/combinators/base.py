from ..transform import Transform, TransformMeta
import copyreg


class CombinatorMeta(TransformMeta):
    @staticmethod
    def reduce(cls):
        return (cls.__creation_values__[0], cls.__creation_values__[1:])


copyreg.pickle(CombinatorMeta, CombinatorMeta.reduce)


class CombinatorBase(Transform, metaclass=CombinatorMeta):
    # TODO: Clean this up by propagating touches to higher levels
    def _warn_if_files_untouched(self):
        pass
