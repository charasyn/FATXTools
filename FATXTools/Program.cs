using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO.MemoryMappedFiles;
using System.IO;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace FATXTools
{
    class FATXFS
    {
        // based on information found here:
        //  http://hackipedia.org/Disk%20formats/File%20systems/FATX,%20File%20Allocation%20Table%20(X-Box)/THE%20XBOX%20HARD%20DRIVE%20(FATX%20description).htm

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Ansi)]
        public struct DirectoryEntry
        {
            public byte NameLength;
            public byte Attribute;
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 42, ArraySubType = UnmanagedType.U1)]
            public byte[] RawName;
            public uint FirstCluster;
            public uint FileSize;
            public ushort ModTime;
            public ushort ModDate;
            public ushort CreateTime;
            public ushort CreateDate;
            public ushort AccessTime;
            public ushort AccessDate;

            public bool ValidFile
            {
                get { return NameLength > 0 && NameLength < 42; }
            }
            public bool IsDirectory
            {
                get { return (Attribute & 0x10) != 0; }
            }
            public string Name
            {
                get { return ValidFile ? Encoding.ASCII.GetString(RawName, 0, NameLength) : (NameLength == 0xe5 ? "<deleted file>" : "<unknown>"); }
                set
                {
                    var b = Encoding.ASCII.GetBytes(value);
                    if (b.Length > 42 || b.Length == 0)
                        throw new ArgumentException("Invalid filename.");
                    NameLength = (byte)b.Length;
                    Buffer.BlockCopy(b, 0, RawName, 0, b.Length);
                }
            }
            public override string ToString()
            {
                return Name + (IsDirectory ? " <dir>" : "");
            }
            public static DirectoryEntry New()
            {
                DirectoryEntry ret = new DirectoryEntry();
                var barr = Enumerable.Repeat((byte)0xff, 42).ToArray();
                ret.RawName = barr;
                return ret;
            }
            public static DirectoryEntry[] FromByteArray(byte[] inp)
            {
                List<DirectoryEntry> dir = new List<DirectoryEntry>();
                GCHandle? gch = null;
                try
                {
                    gch = GCHandle.Alloc(inp, GCHandleType.Pinned);
                    for (int i = 0; i < inp.Length / 64; i++)
                    {
                        var ptr = gch.Value.AddrOfPinnedObject() + i * 64;
                        DirectoryEntry d = (DirectoryEntry)Marshal.PtrToStructure(ptr, typeof(DirectoryEntry));
                        if (d.NameLength == 0xff)
                            break;
                        dir.Add(d);
                    }
                }
                finally
                {
                    if (gch.HasValue)
                        gch.Value.Free();
                }
                return dir.ToArray();
            }

            public static byte[] ToByteArray(DirectoryEntry[] inp)
            {
                byte[] ret = new byte[64 * (inp.Length + 1)];
                GCHandle? gch = null;
                try
                {
                    gch = GCHandle.Alloc(ret, GCHandleType.Pinned);
                    for (int i = 0; i < (inp.Length + 1); i++)
                    {
                        var ptr = gch.Value.AddrOfPinnedObject() + i * 64;
                        if (i < inp.Length)
                            Marshal.StructureToPtr(inp[i], ptr, false);
                        else
                            for (int j = 0; j < 64 / 4; j++)
                            {
                                Marshal.WriteInt32(ptr + j * 4, unchecked((int)0xffffffff));
                            }
                    }
                }
                finally
                {
                    if (gch.HasValue)
                        gch.Value.Free();
                }

                return ret;
            }
        }

        static long[] offsets = new long[] { 0x00080000, 0x2ee80000, 0x5dc80000, 0x8ca80000, 0xabe80000 };
        static long[] sizes = new long[] { 0x2ee00000, 0x2ee00000, 0x2ee00000, 0x1f400000, 0x132000000 };

        FileStream file;
        BinaryWriter w;
        BinaryReader r;
        long offset = -1, size = -1, fileSize;

        long clusterOffset = -1, totalClusters = -1;
        int clusterSize = -1;
        bool FATX32 = false;
        uint[] FAT = new uint[0];

        DirectoryEntry[] curDir = new DirectoryEntry[0];
        Stack<uint> dirStack = new Stack<uint>();
        uint curCluster = 1;

        public FATXFS(string path)
        {
            fileSize = new FileInfo(path).Length;
            file = new FileStream(path, FileMode.Open);
            w = new BinaryWriter(file);
            r = new BinaryReader(file);
        }

        private void RS(long l) { file.Seek(l + offset, SeekOrigin.Begin); }
        private long ClusterLoc(uint n)
        {
            return (clusterSize * (n - 1) + clusterOffset);
        }
        private byte[] ReadCluster(uint n)
        {
            RS(ClusterLoc(n));
            byte[] clustBuf = new byte[clusterSize];
            r.Read(clustBuf, 0, clusterSize);
            return clustBuf;
        }
        private byte[] ReadClusterSeries(uint n)
        {
            List<byte[]> bufs = new List<byte[]>();
            var curN = n;
            while (true)
            {
                var tmp = ReadCluster(curN);
                bufs.Add(tmp);
                if ((FAT[curN] >= (FATX32 ? 0xfffffff0 : 0xfff0)))
                    break;
                curN = FAT[curN];
            }
            int i = 0;
            return bufs.Aggregate(new byte[bufs.Count * clusterSize], (acc, t) =>
            {
                Buffer.BlockCopy(t, 0, acc, clusterSize * (i++), clusterSize);
                return acc;
            });
        }
        private uint FindFile(string s)
        {
            var matches = curDir.Where((n) => n.Name.ToLower() == s.ToLower()).ToArray();
            if (matches.Length != 1)
            {
                throw new ArgumentException("File does not exist.");
            }
            var clus = matches[0].FirstCluster;
            return clus;
        }
        private void SaveFAT()
        {
            RS(0x1000);
            for (int i = 0; i < totalClusters; i++)
            {
                if (FATX32)
                    w.Write((uint)FAT[i]);
                else
                    w.Write((ushort)FAT[i]);
            }
            w.Flush();
        }
        private void Mount()
        {
            Mount(0, fileSize);
        }
        private void Mount(int partNum)
        {
            if (partNum < 0 || partNum >= offsets.Length)
            {
                throw new ArgumentException("Invalid drive.");
            }
            Mount(offsets[partNum], sizes[partNum]);
        }
        private void Mount(long offset, long size)
        {
            this.offset = offset;
            this.size = size;
            {
                byte[] sig = new byte[4];
                RS(0);
                r.Read(sig, 0, 4);
                if (!sig.SequenceEqual(new byte[] { (byte)'F', (byte)'A', (byte)'T', (byte)'X' }))
                {
                    throw new Exception("Invalid partition signature.");
                }
            }

            // calculate # of clusters

            RS(8);
            clusterSize = r.ReadInt32() * 512;
            totalClusters = size / clusterSize;
            FATX32 = (totalClusters > 65525);
            clusterOffset = 0x1000 + totalClusters * (FATX32 ? 4 : 2);
            if (clusterOffset % 0x1000 != 0)
                clusterOffset += 0x1000 - clusterOffset % 0x1000;

            // load FAT
            RS(0x1000);
            FAT = new uint[totalClusters];
            for (int i = 0; i < totalClusters; i++)
            {
                if (FATX32)
                    FAT[i] = r.ReadUInt32();
                else
                    FAT[i] = r.ReadUInt16();
            }

            ReadRootDirectory();

        }
        private void ReadRootDirectory()
        {
            var rootDirectory = DirectoryEntry.FromByteArray(ReadCluster(1));
            curDir = rootDirectory;
            dirStack = new Stack<uint>();
            curCluster = 1;
        }
        private void PruneDirectory()
        {
            // TODO: Make sure that this maintains the order of the entries.
            curDir = curDir.Where((de) => de.ValidFile).ToArray();
        }
        private void WriteCluster(uint clus, byte[] data)
        {
            RS(ClusterLoc(clus));
            w.Write(data, 0, Math.Min(data.Length, clusterSize));
        }
        private void WriteClusterSeries(uint firstClus, byte[] data)
        {
            uint curClus = firstClus;
            int i = 0;
            do
            {
                RS(ClusterLoc(curClus));
                w.Write(data, i * clusterSize, Math.Min(data.Length - i * clusterSize, clusterSize));
                curClus = FAT[curClus];
                i++;
            } while (curClus < (FATX32 ? 0xfffffff0 : 0xfff0));
        }
        private void SaveDirectory()
        {
            WriteClusterSeries(curCluster, Enumerable.Repeat((byte)0xff, FindDirSize()).ToArray());
            WriteClusterSeries(curCluster, DirectoryEntry.ToByteArray(curDir));
        }
        private int FindDirSize()
        {
            int i = 1;
            var curClus = curCluster;
            for (; FAT[curClus] < (FATX32 ? 0xfffffff0 : 0xfff0); i++, curClus = FAT[curClus]) ;
            return i * clusterSize;
        }
        public bool FileExists(string s)
        {
            bool exists = false;
            try
            {
                FindFile(s);
                exists = true;
            }
            catch { }
            return exists;
        }
        public DirectoryEntry Stat(string s)
        {
            var matches = curDir.Where((n) => n.Name.ToLower() == s.ToLower()).ToArray();
            if (matches.Length != 1)
            {
                throw new ArgumentException("File does not exist.");
            }
            return matches[0];
        }
        public void ReadFile(string s)
        {
            byte[] bytes;
            var p = Path.GetFileName(s);
            if (!FileExists(p))
                throw new ArgumentException("File does not exist.");
            var de = Stat(p);
            var length = de.FileSize;
            bytes = new byte[length];
            var tmpbytes = ReadClusterSeries(de.FirstCluster);
            Buffer.BlockCopy(tmpbytes, 0, bytes, 0, (int)length);
            File.WriteAllBytes(s, bytes);
        }
        public void WriteFile(string s)
        {
            var fdata = File.ReadAllBytes(s);
            var dirEnt = DirectoryEntry.New();
            dirEnt.Name = Path.GetFileName(s);
            dirEnt.Attribute = 0;
            dirEnt.FileSize = unchecked((uint)(new FileInfo(s).Length));

            if (FileExists(dirEnt.Name))
                throw new Exception("File already exists.");

            var numClus = (dirEnt.FileSize + clusterSize - 1) / clusterSize;
            uint i = 2;
            for (; i < totalClusters; i++)
            {
                bool good = true;
                for (uint j = i; j < (i + numClus) && good; j++)
                {
                    if (FAT[j] != 0)
                        good = false;
                }
                if (good)
                    break;
            }
            if (i == totalClusters)
            {
                throw new Exception("Disk is out of space.");
            }
            var clus = i;
            for (; i < (clus + numClus); i++)
            {
                FAT[i] = (i < (clus + numClus - 1)) ? i + 1 : (FATX32 ? 0xffffffff : 0xffff);
            }
            WriteClusterSeries(clus, fdata);
            dirEnt.FirstCluster = clus;
            var d = new List<DirectoryEntry>();
            d.AddRange(curDir);
            d.Add(dirEnt);
            curDir = d.ToArray();
            SaveDirectory();
            SaveFAT();
        }
        public void CD(string p)
        {
            if (p[0] == '/')
            {
                ReadRootDirectory();
            }
            var spl = p.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var c in spl)
            {
                if (c == ".")
                    continue;
                if (c == "..")
                {
                    if (dirStack.Count == 0)
                        ReadRootDirectory();
                    else
                    {
                        var clus2 = dirStack.Pop();
                        curCluster = clus2;
                        curDir = DirectoryEntry.FromByteArray(ReadClusterSeries(clus2));
                    }
                    continue;
                }
                dirStack.Push(curCluster);
                var clus = FindFile(c);
                curDir = DirectoryEntry.FromByteArray(ReadClusterSeries(clus));
                curCluster = clus;
            }
        }
        public void RM(string p)
        {
            uint clus = 0, nextClus;
            for (int i = 0; i < curDir.Length; i++)
            {
                if (curDir[i].Name.ToLower() == p.ToLower())
                {
                    curDir[i].NameLength = (byte)0xe5;
                    clus = curDir[i].FirstCluster;
                    break;
                }
            }
            if (clus == 0)
                throw new ArgumentException("File does not exist.");
            do
            {
                nextClus = FAT[clus];
                FAT[clus] = 0;
                clus = nextClus;
            } while ((clus < (FATX32 ? 0xfffffff0 : 0xfff0)));
            SaveDirectory();
            SaveFAT();
        }
        public void MKDIR(string p)
        {
            var dirEnt = DirectoryEntry.New();
            dirEnt.Name = p;
            dirEnt.Attribute = 0x10;
            dirEnt.FileSize = 0;
            {
                if (FileExists(p))
                {
                    var tmpDE = Stat(p);
                    if (tmpDE.IsDirectory)
                        return;
                    throw new Exception("File already exists.");
                }
            }
            var numClus = 1;
            uint i = 2;
            for (; i < totalClusters; i++)
            {
                bool good = true;
                for (uint j = i; j < (i + numClus) && good; j++)
                {
                    if (FAT[j] != 0)
                        good = false;
                }
                if (good)
                    break;
            }
            if (i == totalClusters)
            {
                throw new Exception("Disk is out of space.");
            }
            var clus = i;
            FAT[clus] = (FATX32 ? 0xffffffff : 0xffff);
            WriteCluster(clus, Enumerable.Repeat((byte)0xff, clusterSize).ToArray());
            dirEnt.FirstCluster = clus;
            var d = new List<DirectoryEntry>();
            d.AddRange(curDir);
            d.Add(dirEnt);
            curDir = d.ToArray();
            SaveDirectory();
            SaveFAT();
        }
        public void OverwriteFile(string s)
        {
            var p = Path.GetFileName(s);
            if (Directory.Exists(s))
            {
                if (FileExists(p))
                {
                    if (!Stat(p).IsDirectory)
                    {
                        throw new Exception("Filesystem layout was unexpected.");
                    }
                }
                else
                {
                    MKDIR(p);
                }
                CD(p);

                foreach (var t in Directory.EnumerateFileSystemEntries(s))
                {
                    OverwriteFile(t);
                }

                CD("..");
                return;
            }
            if (FileExists(p))
            {
                Console.WriteLine("Removing existing '{0}'...", p);
                RM(p);
            }
            Console.WriteLine("Writing '{0}'...", p);
            WriteFile(s);
        }
        public void OverwriteRecursive(string baseDir)
        {
            foreach (var f in Directory.EnumerateFileSystemEntries(baseDir))
            {
                if (Directory.Exists(f))
                {
                    var p = Path.GetFileName(f);
                    MKDIR(p);
                    CD(p);
                    OverwriteRecursive(f);
                    CD("..");
                }
                else
                {
                    OverwriteFile(f);
                }
            }
        }
        public void Browse()
        {
            bool loop = true;
            while (loop)
            {
                Console.Write("> ");
                var p = Console.ReadLine();
                var spl1 = p.Split(new char[] { ' ' }, 2);
                var command = spl1[0];
                switch (command)
                {
                    case "cd": CD(spl1[1]); break;

                    case "exit":
                    case "quit":
                        loop = false;
                        break;

                    case "ls":
                    case "dir":
                        foreach (var n in curDir.Where((de) => de.ValidFile))
                        {
                            Console.WriteLine("{0} {1}", (n.Attribute & 0x10) == 0 ? "     " : "<dir>", n.Name);
                        }
                        break;

                    default: Console.WriteLine("Unknown command."); break;
                }
            }
        }
        public void CloneFS(string path)
        {
            PruneDirectory();
            foreach (var de in curDir)
            {
                if (de.IsDirectory)
                {
                    Directory.CreateDirectory(path + de.Name);
                    CD(de.Name);
                    CloneFS(path + de.Name + "\\");
                    CD("..");
                }
                else
                {
                    var fdatatmp = ReadClusterSeries(de.FirstCluster);
                    var fdata = new byte[de.FileSize];
                    Buffer.BlockCopy(fdatatmp, 0, fdata, 0, (int)de.FileSize);
                    File.WriteAllBytes(path + de.Name, fdata);
                }
            }
        }
        public void InstallNDURE(string ndure)
        {
            {
                Mount(3);

                // cp -R /CD/ndure/C/xodash/*.xbe /xbox/C/xodash/
                Console.WriteLine("Modifying C drive...");
                CD("/xodash");
                foreach (var s in Directory.EnumerateFiles(ndure + @"C\xodash", "*.xbe"))
                {
                    OverwriteFile(s);
                }

                // cp    /CD/ndure/C/xodash/ernie.xtf /xbox/C/xodash/
                OverwriteFile(ndure + @"C\xodash\ernie.xtf");

                // cp    /CD/ndure/C/xodash/xbox.xtf /xbox/C/xodash/
                OverwriteFile(ndure + @"C\xodash\xbox.xtf");

                // cp -R /CD/ndure/C/media/* /xbox/C/media/
                CD("/");
                if (!FileExists("media"))
                    MKDIR("media");
                CD("/media");
                foreach (var s in Directory.EnumerateFileSystemEntries(ndure + @"C\media"))
                {
                    OverwriteFile(s);
                }

                // cp -R /CD/ndure/C/bios/* /xbox/C/bios/
                CD("/");
                if (!FileExists("bios"))
                    MKDIR("bios");
                CD("/bios");
                foreach (var s in Directory.EnumerateFileSystemEntries(ndure + @"C\bios"))
                {
                    OverwriteFile(s);
                }

                // cp -R /CD/ndure/C/*.xbe /xbox/C/
                CD("/");
                foreach (var s in Directory.EnumerateFiles(ndure + @"C", "*.xbe"))
                {
                    OverwriteFile(s);
                }

                // if [ -f /xbox/C/xboxdashdata.185ead00/settings_adoc.xip ]; then
                //     rm /xbox/C/xboxdashdata.185ead00/settings_adoc.xip
                //     if [ -f /CD/ndure/C/xboxdashdata.185ead00/settings_adoc.xip ]; then
                //         cp -R /CD/ndure/C/xboxdashdata.185ead00/settings_adoc.xip /xbox/C/xboxdashdata.185ead00/
                //     fi
                // fi
                CD("/");
                do
                {
                    if (!FileExists("xboxdashdata.185ead00"))
                        break;
                    CD("/xboxdashdata.185ead00");
                    if (FileExists("settings_adoc.xip"))
                        RM("settings_adoc.xip");
                    if (File.Exists(ndure + @"C\xboxdashdata.185ead00\settings_adoc.xip"))
                        OverwriteFile(ndure + @"C\xboxdashdata.185ead00\settings_adoc.xip");
                } while (false);

                // if [ -f /CD/ndure/C/xboxdashdata.17cdc100/default.xip ]; then 
                //     mkdir -p /xbox/C/xboxdashdata.17cdc100
                //     cp -R /CD/ndure/C/xboxdashdata.17cdc100/* /xbox/C/xboxdashdata.17cdc100/
                // fi
                CD("/");
                do
                {
                    if (!File.Exists(ndure + @"C\xboxdashdata.17cdc100\default.xip"))
                        break;
                    if (!FileExists("xboxdashdata.17cdc100"))
                        MKDIR("xboxdashdata.17cdc100");
                    CD("/xboxdashdata.17cdc100");
                    OverwriteFile(ndure + @"C\xboxdashdata.17cdc100\default.xip");
                } while (false);

                // rm -rf /xbox/C/*.xtf
                CD("/");
                {
                    var remove = curDir.Where((de) => (de.Name.EndsWith(".xtf"))).Select((de) => de.Name).ToArray();
                    foreach (var s in remove)
                    {
                        RM(s);
                    }
                }

                CD("/");
                SaveDirectory();
                SaveFAT();
                // end of C drive shenanigans


                Console.WriteLine("Modifying E drive...");
                Mount(4);

                // cp -R /CD/ndure/E/dash/* /xbox/E/dash/
                CD("/");
                if (!FileExists("dash"))
                    MKDIR("dash");
                CD("/dash");
                foreach (var s in Directory.EnumerateFileSystemEntries(ndure + @"E\dash"))
                {
                    OverwriteFile(s);
                }

                // if [ -f /CD/ndure/E/ndts/default.xbe ]; then
                //     cp -R /CD/ndure/E/ndts/* /xbox/E/ndts/
                // fi
                CD("/");
                do
                {
                    if (!File.Exists(ndure + @"E\ndts\default.xbe"))
                        break;
                    if (!FileExists("ndts"))
                        MKDIR("ndts");
                    CD("/ndts");
                    foreach (var s in Directory.EnumerateFileSystemEntries(ndure + @"E\ndts"))
                    {
                        OverwriteFile(s);
                    }
                } while (false);

                // cp -R /CD/ndure/E/NKP11/* /xbox/E/NKP11/
                CD("/");
                if (!FileExists("NKP11"))
                    MKDIR("NKP11");
                CD("/NKP11");
                foreach (var s in Directory.EnumerateFileSystemEntries(ndure + @"E\NKP11"))
                {
                    OverwriteFile(s);
                }

                // if [ -f /CD/ndure/E/TDATA/fffe0000/music/ST.DB ]; then
                //     if [ -f /xbox/E/TDATA/fffe0000/music/ST.DB ]; then
                //         cp -R /xbox/E/TDATA/fffe0000/music/ST.DB /xbox/E/TDATA/fffe0000/music/ST2.DB
                //         rm /xbox/E/TDATA/fffe0000/music/ST.DB
                //     fi 
                //     cp -R /CD/ndure/E/TDATA/fffe0000/music/ST.DB /xbox/E/TDATA/fffe0000/music/
                // fi
                Action<string> mkcd = (s) =>
                {
                    if (!FileExists(s))
                        MKDIR(s);
                    CD(s);
                };
                CD("/");
                do
                {
                    if (!File.Exists(ndure + @"E\TDATA\fffe0000\music\ST.DB"))
                        break;
                    mkcd("TDATA");
                    mkcd("fffe0000");
                    mkcd("music");
                    OverwriteFile(ndure + @"E\TDATA\fffe0000\music\ST.DB");
                } while (false);

                CD("/");
                SaveDirectory();
                SaveFAT();
                // end of E drive shenanigans
            }
        }

        static void Usage()
        {
            // TODO: this
        }
        static void Main(string[] args)
        {
            FATXFS fs;
            if (args.Length < 1)
            {
                Usage();
                return;
            }
            switch (args[0].ToLower())
            {
                case "ndure":
                    if (args.Length != 3) { Usage(); return; }
                    if (!File.Exists(args[1]) || !Directory.Exists(args[2])) { Usage(); return; }
                    if (!Directory.Exists(args[2]+"/C") || !Directory.Exists(args[2]+"/E"))
                    {
                        Console.WriteLine("Invalid NDURE folder.");
                        Usage();
                        return;
                    }
                    fs = new FATXFS(args[1]);
                    fs.InstallNDURE(args[2]);
                    break;
                case "browse":
                    if (args.Length < 2 || args.Length > 4) { Usage(); return; }
                    if (!File.Exists(args[1])) { Usage(); return; }
                    fs = new FATXFS(args[1]);
                    switch (args.Length)
                    {
                        case 2: fs.Mount(); break;
                        case 3:
                            int partNum;
                            if (!int.TryParse(args[2], out partNum))
                            {
                                Console.WriteLine("Invalid partition number.");
                                Usage();
                                return;
                            }
                            fs.Mount(partNum);
                            break;
                        case 4:
                            long offset, size;
                            if (!long.TryParse(args[2], out offset) || !long.TryParse(args[3], out size))
                            {
                                Console.WriteLine("Invalid offset or size.");
                                Usage();
                                return;
                            }
                            fs.Mount(offset, size);
                            break;
                    }
                    fs.Browse();
                    break;
                case "export":

                    break;
                case "import":

                    break;
            }
        }
    }
}

