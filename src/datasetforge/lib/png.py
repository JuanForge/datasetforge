import io
from PIL import Image

def encode(data: bytes) -> bytes:
    img = Image.open(io.BytesIO(data)).convert("RGB")
    
    out = io.BytesIO()
    img.save(out, "PNG", compress_level=9)
    return out.getvalue()