from ..transform import TransformMeta
from ..workarounds.handprint import Handprint
import copyreg


class CombinatorMeta(TransformMeta):
    @staticmethod
    def produce(hp):
        return hp.produce()

    @staticmethod
    def reduce(c):
        return (CombinatorMeta.produce, (Handprint(c),))


copyreg.pickle(CombinatorMeta, CombinatorMeta.reduce)
