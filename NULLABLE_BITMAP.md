# Nullable Bitmap

`hat/hatDataStructure` provides `NullableBitmap` for column nullability with
one bit per row instead of one byte or pointer per value.

```go
bitmap, err := hatDataStructure.NewNullableBitmap(rowCount)
if err != nil {
    return err
}
_ = bitmap.SetNull(row)
isNull, err := bitmap.IsNull(row)
nullCount := bitmap.CountNulls()
```

`SetNull` and `SetValid` update a single bit. `Resize` preserves flags in the
overlapping row range, initializes new rows as valid, and clears discarded
bits so later growth cannot resurrect stale null values. `CountNulls` uses
word-level population counts.

The bitmap is not synchronized; callers sharing one instance concurrently
must provide synchronization. Invalid lengths return
`ErrNullableBitmapInvalid`, and invalid row indexes return
`ErrNullableBitmapIndexOutOfRange`. Operations do not allocate after the
bitmap has enough capacity for the requested size.
