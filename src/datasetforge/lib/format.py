import io

from PIL import Image


class errors:
    class UnsupportedAnimation(Exception):
        pass

def encode(data: bytes, format: str) -> bytes:
    img = Image.open(io.BytesIO(data)).convert("RGB")
    
    if getattr(img, "n_frames", 1) > 1 or getattr(img, "is_animated", False):
        raise errors.UnsupportedAnimation()
    
    out = io.BytesIO()
    img.save(
        out,
        format.upper(),
        compress_level=9,
        #optimize=True
    )
    return out.getvalue()