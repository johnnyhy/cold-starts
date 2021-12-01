import numpy as np 
import scipy.stats as stats


def truncated_normal_distribution(mu, lower, upper, sigma, size_):
    data = stats.truncnorm.rvs(
        (lower - mu) / sigma, (upper - mu) / sigma, loc=mu, scale=sigma, size = size_)
    
    return data

def normal_distribution(mu, sigma, size_):
    data = np.random.normal(loc=mu,scale=sigma,size=size_)

    return data
