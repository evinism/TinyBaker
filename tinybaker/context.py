class BakerContext:
    def __init__(self, overwrite=False):
        self.overwrite = overwrite


class DefaultContext(BakerContext):
    pass
