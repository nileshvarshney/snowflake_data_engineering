import sys

def f_to_celsius_udf(temp_fahrenheit: float) -> float:
    temp_celsius = (float(temp_fahrenheit) - 32) * 5.0/9.0
    return temp_celsius

if __name__ == "__main__":
    if len(sys.argv) > 1:
        print(f_to_celsius_udf(*sys.argv[1:]))
    else:
        print(f_to_celsius_udf())
