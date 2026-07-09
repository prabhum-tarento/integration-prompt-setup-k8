namespace IIS.WMS.Consumer.Domain.Exceptions;

/// <summary>
/// Base type for exceptions that represent a broken business invariant. Translated to Problem
/// Details only at the Api boundary (see aspnet-rest-apis.instructions.md) - never at Domain or
/// Application layer.
/// </summary>
public abstract class DomainException : Exception
{
    /// <summary>Creates the exception with the given message; derived types build the message from their own specific context.</summary>
    protected DomainException(string message)
        : base(message)
    {
    }
}
