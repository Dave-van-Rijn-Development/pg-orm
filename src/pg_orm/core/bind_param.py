class BindParam:
    def __init__(self, name: str):
        self.name = name


def bind_param(*, name: str) -> BindParam:
    return BindParam(name)
