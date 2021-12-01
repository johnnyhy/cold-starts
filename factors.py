#!/usr/bin/env python2.7
from distribution import *

def cold_start_from_memory(memory, language=None):
    # TODO: potentially determine cutoff for memory's impact on cold start times.
    #       this is to prevent cold start values from going negative when
    #       system memory is very high
    # these defaults are just the average of all different scenarios
    cold_start = 1000  # milliseconds
    memory_factor = -0.16

    if language == "Java":
        cold_start = 1634
        memory_factor = -0.25
    if language == "JS":
        cold_start = 606
        memory_factor = -0.07

    return cold_start + (memory_factor * memory)


def cold_start_from_package_size(platform, language, package_size):
    cold_start = 9599  # milliseconds
    package_size_factor = 14.25
    if platform == "AWS" and language == "Java":
        cold_start = 1510
        package_size_factor = 9
    if platform == "AWS" and language == "JS":
        cold_start = 613
        package_size_factor = 12
    if platform == "Azure" and language == "Java":
        # outlier: why would increased application size correspond
        # to decreased cold start time? Might be worth investigating.
        cold_start = 25580
        package_size_factor = -7
    if platform == "Azure" and language == "JS":
        cold_start = 8571
        package_size_factor = 43

    return cold_start + (package_size_factor * package_size)

def cpu_utlization(size, mu=None, lower=None, upper=None, sigma=None):

    if mu and lower and upper and sigma:
        return truncated_normal_distribution(mu, lower, upper, sigma, size)
    else:
        return truncated_normal_distribution(66.9, 0, 100, 16, size)
        