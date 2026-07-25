# Designed for Datasets of Any Size

This project is designed to work efficiently with both small datasets and extremely large collections, scaling up to datasets containing billions of files.

Its goal is to enable dataset analysis and optimization at any scale. Removing duplicate and redundant data improves the overall quality of a dataset, provides a more accurate representation of the data available for training, and reduces unexpected issues during model training.

The `--stream` mode prioritizes minimal memory usage, making it suitable for datasets that exceed the available system memory. As a trade-off, some operations—such as duplicate detection—can become significantly slower on very large collections.

When `--stream` is not used, performance is generally much higher while still maintaining a very small memory footprint. In my tests, processing datasets containing tens of thousands of images required only about **4 MiB** of RAM.

**In summary:**

* **Small datasets:** Fast processing with low memory usage.
* **Large datasets:** High performance while maintaining a very small memory footprint.
* **Massive datasets:** Supported through `--stream`, at the cost of longer execution times for certain operations.

# WARNING
> [!WARNING]
> **Experimental Project**
>
> This project is currently under active development and should be considered **experimental**. Breaking changes, bugs, and unexpected behavior may occur between releases.
>
> At this time, the project has **only been tested on Linux**. While some features may work on other operating systems, **compatibility is not guaranteed** until they have been officially tested and validated.
>
> If you encounter any issues, please report them through the **Issues** page.
