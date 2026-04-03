namespace CommandFramework.Core;

/// <summary>
/// Represents either a successful value or a string error description.
/// </summary>
public readonly struct Result<T>
{
    private readonly T? _value;
    private readonly string? _error;

    public bool IsSuccess { get; }
    public bool IsError => !IsSuccess;

    public T Value => IsSuccess
        ? _value!
        : throw new InvalidOperationException($"Result is an error: {_error}");

    public string Error => !IsSuccess
        ? _error!
        : throw new InvalidOperationException("Result is a success.");

    private Result(T value)      { _value = value; IsSuccess = true; }
    private Result(string error) { _error = error; IsSuccess = false; }

    public static Result<T> Ok(T value)        => new(value);
    public static Result<T> Fail(string error) => new(error);

    /// <summary>
    /// Implicit conversion from a value — enables returning a value directly.
    /// </summary>
    public static implicit operator Result<T>(T value) => Ok(value);

    /// <summary>
    /// Implicit conversion from a string — enables returning an error directly.
    /// </summary>
    public static implicit operator Result<T>(string error) => Fail(error);

    public TOut Match<TOut>(Func<T, TOut> onSuccess, Func<string, TOut> onError)
        => IsSuccess ? onSuccess(_value!) : onError(_error!);

    public override string ToString()
        => IsSuccess ? $"Ok({_value})" : $"Error({_error})";
}
