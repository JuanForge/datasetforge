import os
import time

import av

base = os.path.dirname(os.path.abspath(__file__))
os.chdir(base)

start_time = time.monotonic()
container = av.open("./private/source/0_H.264_HP.mp4")

for i, frame in enumerate(container.decode(video=0)):
    if i % 80 == 0:
        image = frame.to_image()
        image.save(f"./private/out/frames/{i:06d}.png")

print(time.monotonic() - start_time)