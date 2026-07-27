from pathlib import Path

RuntimeWarning("not maintained.")

def ext(mode: int, file: Path) -> str:
    if mode == 0:
        return "png"
    elif mode == 1:
        return file.suffix.replace(".", "")
    else:
        raise RuntimeError("invalide mode")