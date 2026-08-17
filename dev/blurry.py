import heapq
import math
import os
import sys

import cv2

from datasetforge.lib.source import sourceGen

base = os.path.dirname(os.path.abspath(__file__))
os.chdir(base)

top = []
bottom = []
k = 80


def resize_pixel_area(image: cv2.Mat, resolution: int, exceptR: bool = True):
    height, width = image.shape[:2]
    if (height * width) < resolution ** 2:
        if exceptR:
            raise RuntimeError("resolution_input < resolution_target")
        else:
            return image
    
    scale = resolution / math.sqrt(width * height)
    
    new_width = round(width * scale)
    new_height = round(height * scale)
    
    return cv2.resize(
        image,
        (new_width, new_height),
        interpolation=cv2.INTER_AREA,
    )




use_resize_pixel_area: bool = False
for index, file in enumerate(
    sourceGen("./private/temp/out/tmp/0", recursive=True, exclude_dirs=None)
):
    sys.stdout.write(f"{index} : {os.path.abspath(str(file))} ...")
    
    image = cv2.imread(file, cv2.IMREAD_GRAYSCALE)
    print(image.shape)
    if use_resize_pixel_area:
        image = resize_pixel_area(image, resolution=512, exceptR=True)
    
    print(image.shape)
    # pyrefly: ignore [no-matching-overload]
    laplacian = cv2.Laplacian(image, cv2.CV_64F)
    score = laplacian.var()
    
    item = (score, file, image.shape[:2])
    
    if len(top) < k:
        heapq.heappush(top, item)
    elif score > top[0][0]:
        heapq.heapreplace(top, item)
    
    item = (-score, file, image.shape[:2])
    
    if len(bottom) < k:
        heapq.heappush(bottom, item)
    elif score < -bottom[0][0]:
        heapq.heapreplace(bottom, item)


top.sort(reverse=True)
bottom.sort(reverse=False)

print("\n--- best ---")
for score, file, rs in top:
    print(score, os.path.abspath(file), rs)

print("\n--- PIRES ---")
for score, file, rs in bottom:
    print(-score, os.path.abspath(file), rs)