from ..transform import TransformMeta
import copyreg


class CombinatorMeta(TransformMeta):
    @staticmethod
    def reduce(cls):
        return (cls.__creation_values__[0], cls.__creation_values__[1:])


copyreg.pickle(CombinatorMeta, CombinatorMeta.reduce)
