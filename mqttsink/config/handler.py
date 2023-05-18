from typing import List, Any
import tomllib


class ConfigurationError(Exception):
    pass


class Parameter:
    def __init__(
        self,
        name: str,
        argtype: type,  # pylint: disable=W0622
        required: bool = False,
        default: Any = None,
    ) -> None:
        self.name = name
        self.argtype = argtype
        self.required = required
        self.default = self.cast(default)

    def __repr__(self) -> str:
        return f"{self.name} ({self.argtype})"

    def get_from(self, raw: dict) -> Any:
        try:
            return self.cast(raw[self.name])
        except KeyError as err:
            if self.required:
                raise ConfigurationError(
                    f"Missing mandatory parameter : {self}"
                ) from err
            return self.default

    def cast(self, argument) -> Any:
        # None exception
        if argument is None:
            return argument
        # Cast
        try:
            return self.argtype(argument)
        except (ValueError, TypeError) as err:
            raise ConfigurationError(
                f"Cannot use [{argument}] as [{self.argtype.__name__}]"
            ) from err


def load(path: str) -> dict:
    with open(path, "rb") as config_file:
        return tomllib.load(config_file)


def extract(raw: dict, mapper: List[Parameter]) -> dict:
    return {item.name: item.get_from(raw) for item in mapper}
