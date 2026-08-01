from typing import TextIO


class PrefixWriter:
    def __init__(self, stdout: TextIO, prefix: str) -> None:
        self.stdout = stdout
        self.prefix = prefix
        self.start = True

    def write(self, text: str) -> None:
        if self.start and text:
            text = self.prefix + text

        self.stdout.write(text)
        self.start = text.endswith("\n")

    def flush(self) -> None:
        self.stdout.flush()

    def __getattr__(self, name: str) -> None:
        return getattr(self.stdout, name)