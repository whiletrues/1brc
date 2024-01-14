using System.Runtime.InteropServices;

namespace _1brc;

[StructLayout(LayoutKind.Sequential, Size = 16)]
public  struct Aggregation
{
    public long Sum;
    public nint Count;
    public short Min;
    public short Max;
    public double Average => (double)Sum / Count;

    public Aggregation(nint value)
    {
        Sum += value;
        Count++;
        Min = (short)value;
        Max = (short)value;
    }

    public override string ToString() => $"{Min / 10.0:N1}/{Average / 10.0:N1}/{Max / 10.0:N1}";

    public void Update(nint value)
    {
        Sum += value;

        Count++;

        if (value < Min)
        {
            Min = (short)value;
        }

        if (value > Max)
        {
            Max = (short)value;
        }

        Sum += value;
    }

    public void Merge(Aggregation aggregation)
    {
        Sum += aggregation.Sum;

        Count++;

        if (aggregation.Min < Min)
        {
            Min = (short)aggregation.Min;
        }

        if (aggregation.Max > Max)
        {
            Max = (short)aggregation.Max;
        }

        Sum += aggregation.Sum;
    }
}