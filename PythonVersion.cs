
using System.Runtime.InteropServices;
using System;
using System.Diagnostics;
using System.Text.RegularExpressions;
using static System.Runtime.InteropServices.RuntimeInformation;

[DebuggerDisplay("{GetDebuggerDisplay(),nq}")]

/// <summary>
/// Helper class to parse Python version string generated by pyenv-win.
/// </summary>
[DebuggerDisplay("{" + nameof(GetDebuggerDisplay) + "(),nq}")]
public class PythonVersion : IComparable, IComparable<PythonVersion>, IEquatable<PythonVersion>, ICloneable
{
    private Architecture _arch;

    private Version _version;

    private static readonly Regex _versionIdPattern = new Regex(@"v?(?:(?:(?<epoch>[0-9]+)!)?(?<release>[0-9]+(?:\.[0-9]+)*)(?<pre>[-_\.]?(?<pre_l>(a|b|c|rc|alpha|beta|pre|preview))[-_\.]?(?<pre_n>[0-9]+)?)?(?<post>(?:-(?<post_n1>[0-9]+))|(?:[-_\.]?(?<post_l>post|rev|r)[-_\.]?(?<post_n2>[0-9]+)?))?(?<dev>[-_\.]?(?<dev_l>dev)[-_\.]?(?<dev_n>[0-9]+)?)?)(?:\+(?<local>[a-z0-9]+(?:[-_\.][a-z0-9]+)*))?(?<win32>-win32)?");
        

    public PythonVersion(Version version, Architecture architecture)
    {
        this._arch = architecture;
        this._version = version;
    }
    public PythonVersion(Version version)
    {
        this._version = version;
        this._arch = Architecture.X64;
    }
    /// <summary>
    /// Python version identifiers use the following scheme:
    /// 
    /// [N!]N(.N)*[{a|b|rc}N][.postN][.devN]
    /// 
    /// Version identifiers are separated into up to five segments:
    /// - Epoch segment: N!
    /// - Release segment: N(.N)*
    /// - Pre-release segment: {a|b|rc}N
    /// - Post-release segment: .postN
    /// - Development release segment: .devN
    /// 
    /// Any given release will be a "final release", "pre-release", "post-release" or "developmental release" as defined in the following sections.
    /// All numeric components MUST be non-negative integers represented as sequences of ASCII digits.
    /// ll numeric components MUST be interpreted and ordered according to their numeric value, not as text strings.
    /// All numeric components MAY be zero. Except as described below for the release segment, a numeric component of zero has no special significance aside from always being the lowest possible value in the version ordering.
    /// </summary>
    /// <param name="version"></param>
    public PythonVersion(string version)

    {
        if (version is null)
        {
            throw new ArgumentNullException(nameof(version));
        }
        // Adapted Python version pattern from pypa/packaging https://github.com/pypa/packaging/blob/16.7/packaging/version.py#L159
        Match versionIdMatch = _versionIdPattern.Match(version);
        this._version = Version.Parse(versionIdMatch.Groups["release"].Value);
        this._arch = (versionIdMatch.Groups["win32"].Value).Equals(String.Empty) ? Architecture.X64 : Architecture.X86;
    }

    public PythonVersion(string version, string arch){

    }

    public void SetArchitecture(string arch){
        this._arch = Regex.IsMatch(arch,"32") ? Architecture.X86 : Architecture.X64;
    }

    public string GetArchitecture(){
        return this._arch == Architecture.X86 ? "win32" : "x64";
    }

    public void SetVersion(string version)
    {
        Match versionIdMatch = _versionIdPattern.Match(version);
        this._version = Version.Parse(versionIdMatch.Groups["release"].Value);
        this._arch = (versionIdMatch.Groups["win32"].Value).Equals("") ? Architecture.X64 : Architecture.X86;
    }

    public void SetVersion(Version version){
        this._version = version;
    }

    public string GetVersion(){
        return this.ToString();
    }

    public object Clone(){
        return new PythonVersion(this._version,this._arch);
    }
    public int CompareTo(PythonVersion obj) => this.CompareTo(obj as Object);

    public override string ToString() => this._version.ToString() + ((this._arch == Architecture.X64) ? "" : "-win32");

    // override object.Equals
    public override bool Equals(object obj)
    {
        //
        // See the full list of guidelines at
        //   http://go.microsoft.com/fwlink/?LinkID=85237
        // and also the guidance for operator== at
        //   http://go.microsoft.com/fwlink/?LinkId=85238
        //

        if (obj == null || GetType() != obj.GetType())
        {
            return false;
        }
        PythonVersion otherPyVer = obj as PythonVersion;
        if (otherPyVer != null)
            return this._version.Equals(otherPyVer._version) && this._arch.Equals(otherPyVer._arch);
        else
            throw new ArgumentException("Object is not a Python Version");
    }

    public bool Equals(PythonVersion obj)
    {
        if (obj is not null)
        {
            return this.Equals(obj as Object);
        }
        throw new ArgumentNullException(nameof(obj));
    }

    // override object.GetHashCode
    public override int GetHashCode()
    {
        return ShiftAndWrap(this._version.GetHashCode(), 2) ^ this._arch.GetHashCode();
    }
    private int ShiftAndWrap(int value, int positions)
    {
        positions = positions & 0x1F;

        // Save the existing bit pattern, but interpret it as an unsigned integer.
        uint number = BitConverter.ToUInt32(BitConverter.GetBytes(value), 0);
        // Preserve the bits to be discarded.
        uint wrapped = number >> (32 - positions);
        // Shift and wrap the discarded bits.
        return BitConverter.ToInt32(BitConverter.GetBytes((number << positions) | wrapped), 0);
    }
    public int CompareTo(Object obj)
    {
        if (obj == null) return 1;
        PythonVersion otherPyVer = obj as PythonVersion;
        if (otherPyVer != null)
            return this._version.CompareTo(otherPyVer._version);
        else
            throw new ArgumentException("Object is not a Python Version");
    }

    
    private string GetDebuggerDisplay()
    {
        return ToString();
    }
}