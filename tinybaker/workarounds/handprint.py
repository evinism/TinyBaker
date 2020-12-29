# handprint is a pickleable version of the combinator classes...

# To remove the weird issue of value is none vs. no value
# Use case for Maybe<Maybe<T>>!!
class BasicWrap:
    def __init__(self, value):
        self.value = value


# Turn custom classes with __creation_values__ into something pickleable.
class Handprint:
    def __init__(self, src):
        # Recurse into lists, because we sometimes store combinators in lists for creation_values.
        # This is terrible terrible code
        if isinstance(src, list):
            self._list = BasicWrap([Handprint(val) for val in src])
        elif not hasattr(src, "__creation_values__"):
            # Assume it's pickleable as-is if it doesn't have __creation__values__
            self._unmodified = True
            self._list = None
            self._src = BasicWrap(src)
            self._hcv = None
        else:
            hcv = [Handprint(val) for val in src.__creation_values__]
            self._list = None
            self._src = None
            self._hcv = BasicWrap(hcv)

    def produce(self):
        if self._list:
            return [val.produce() for val in self._list.value]
        if self._src:
            return self._src.value
        creation_values = [val.produce() for val in self._hcv.value]
        return creation_values[0](*creation_values[1:])
