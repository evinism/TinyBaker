class BakerRuntime:
    def __init__(self, overwrite=False):
        self.overwrite = overwrite

    def run(self, transform):
        transform.build(self)


class DefaultRuntime(BakerRuntime):
    pass
