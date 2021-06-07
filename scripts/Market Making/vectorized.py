import numpy
from numba import guvectorize

def ewma(data, alpha, offset=None, dtype=None, order='C', out=None):
    """
    Calculates the exponential moving average over a vector.
    Will fail for large inputs.
    :param data: Input data
    :param alpha: scalar float in range (0,1)
        The alpha parameter for the moving average.
    :param offset: optional
        The offset for the moving average, scalar. Defaults to data[0].
    :param dtype: optional
        Data type used for calculations. Defaults to float64 unless
        data.dtype is float32, then it will use float32.
    :param order: {'C', 'F', 'A'}, optional
        Order to use when flattening the data. Defaults to 'C'.
    :param out: ndarray, or None, optional
        A location into which the result is stored. If provided, it must have
        the same shape as the input. If not provided or `None`,
        a freshly-allocated array is returned.
    """
    data = numpy.array(data, copy=False)

    if dtype is None:
        if data.dtype == numpy.float32:
            dtype = numpy.float32
        else:
            dtype = numpy.float64
    else:
        dtype = numpy.dtype(dtype)

    if data.ndim > 1:
        # flatten input
        data = data.reshape(-1, order)

    if out is None:
        out = numpy.empty_like(data, dtype=dtype)
    else:
        assert out.shape == data.shape
        assert out.dtype == dtype

    if data.size < 1:
        # empty input, return empty array
        return out

    if offset is None:
        offset = data[0]

    alpha = numpy.array(alpha, copy=False).astype(dtype, copy=False)

    # scaling_factors -> 0 as len(data) gets large
    # this leads to divide-by-zeros below
    scaling_factors = numpy.power(1. - alpha, numpy.arange(data.size + 1, dtype=dtype),
                               dtype=dtype)
    # create cumulative sum array
    numpy.multiply(data, (alpha * scaling_factors[-2]) / scaling_factors[:-1],
                dtype=dtype, out=out)
    numpy.cumsum(out, dtype=dtype, out=out)

    # cumsums / scaling
    out /= scaling_factors[-2::-1]

    if offset != 0:
        offset = numpy.array(offset, copy=False).astype(dtype, copy=False)
        # add offsets
        out += offset * scaling_factors[1:]

    return out


@guvectorize(['void(float64[:], float64[:], float64[:])'], '(n),()->(n)', nopython=True)
def _rolling_min(a, window_arr, out):
    window_width = int(window_arr[0])
    
    min_cache = numpy.inf
    min_cache_loc = numpy.nan

    for i in range(0, window_width):
        if ~numpy.isnan(a[i]) and a[i] < min_cache:
            min_cache = a[i]
            min_cache_loc = i
    out[window_width-1] = min_cache 

    for i in range(window_width, len(a)):
        if min_cache_loc >= (i - window_width + 1):
            if ~numpy.isnan(a[i]) and a[i] < min_cache:
                min_cache = a[i]
                min_cache_loc = i
            
        else:
            min_cache = numpy.inf
            min_cache_loc = numpy.nan
            for k in range(i - window_width + 1, i + 1):
                if ~numpy.isnan(a[i]) and a[k] < min_cache:
                    min_cache = a[k]
                    min_cache_loc = k

        out[i] = min_cache 


@guvectorize(['void(float64[:], float64[:], float64[:])'], '(n),()->(n)', nopython=True)
def _rolling_max(a, window_arr, out):
    window_width = int(window_arr[0])

    max_cache = -numpy.inf
    max_cache_loc = numpy.nan

    for i in range(0, window_width):
        if ~numpy.isnan(a[i]) and a[i] > max_cache:
            max_cache = a[i]
            max_cache_loc = i
    out[window_width-1] = max_cache 

    for i in range(window_width, len(a)):
        if max_cache_loc >= (i - window_width + 1):
            if ~numpy.isnan(a[i]) and a[i] > max_cache:
                max_cache = a[i]
                max_cache_loc = i
            
        else:
            max_cache = -numpy.inf
            max_cache_loc = numpy.nan
            for k in range(i - window_width + 1, i + 1):
                if ~numpy.isnan(a[i]) and a[k] > max_cache:
                    max_cache = a[k]
                    max_cache_loc = k

        out[i] = max_cache 


def channel(vals, lookback_hi, lookback_lo = None):
    if lookback_lo is None:
        lookback_lo = lookback_hi

    max_lookback = max(lookback_hi, lookback_lo)

    min_r = _rolling_min(vals, int(lookback_lo))[int(max_lookback)-1:] + 1e-8
    max_r = _rolling_max(vals, int(lookback_hi))[int(max_lookback)-1:] - 1e-8

    buy_signal = vals[max_lookback-1:] > max_r
    sell_signal = vals[max_lookback-1:] < min_r

    buy_signal = numpy.where(buy_signal == False, numpy.nan, buy_signal)
    sell_signal = -numpy.where(sell_signal == False, numpy.nan, sell_signal)

    signal = numpy.nansum(numpy.dstack((buy_signal, sell_signal)), 2)
    
    # prior to numpy 1.9, nansum would have returned numpy.nan (which we want)
    # for summing two nans; now it returns zero
    signal = numpy.where(numpy.abs(signal) < 1e-8, numpy.nan, signal)

    return signal