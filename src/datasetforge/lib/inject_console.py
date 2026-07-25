import sys
class PrefixWriter:
    def __init__(self, stdout: sys.stdout, prefix) -> None:
        self.stdout = stdout
        self.prefix = prefix
        self.start = True

    def write(self, text):
        if self.start and text:
            text = self.prefix + text

        self.stdout.write(text)
        self.start = text.endswith("\n")

    def flush(self):
        self.stdout.flush()

    def __getattr__(self, name):
        return getattr(self.stdout, name)