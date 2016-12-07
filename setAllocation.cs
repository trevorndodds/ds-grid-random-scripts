using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using DataSynapse.GridServer.Driver;
using DataSynapse.GridServer.Admin;
using DataSynapse.GridServer.Engine;

namespace ConsoleApp
{
    class setAllocation
    {
        public static void Main(string[] args)
        {
            var newcsvfile = "";
            var curcsvfile = "";
            //    Console.WriteLine("Number of command line parameters = {0}", args.Length);
            for (int i = 0; i < args.Length; i++)
            {
                Console.WriteLine("Arg[{0}] = [{1}]", i, args[i]);
            }

            if (args.Length > 0)
            {
                //  Console.WriteLine("Args Found: " + args[0]);
                string arg = args[0];
                if (arg == "target")
                {
                    newcsvfile = "newAlloc.csv";
                    curcsvfile = "curAlloc.csv";
                }
                else if (arg == "revert")
                {
                    newcsvfile = "curAlloc.csv";
                    curcsvfile = "newAlloc.csv";
                }
                else { Environment.Exit(0); }

            }
            else
            {
                Console.WriteLine("Usage: setAllocation.exe target");
                Console.WriteLine("Usage: setAllocation.exe revert");
                Environment.Exit(0);
            }

            string proddirector = Convert.ToString(0000000000);
            Console.WriteLine("Connecting to Production GRID");
            DriverManager.Connect(proddirector);

            //Get Current Allocation

            DateTime newTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            TextWriter tf = new StreamWriter(curcsvfile, false);

            Array Brokers = AdminManager.BrokerAdmin.GetAllBrokerInfo();
            foreach (BrokerInfo line in Brokers)
            {
                tf.WriteLine(line.Name + "," + line.BrokerId + "," + line.MinEngines);
            }

            tf.Close();

            //Read Allocation from File and Update Broker Configuration

            string[] file = System.IO.File.ReadAllLines(@newcsvfile);
            foreach (string aline in file)
            {
                string[] sline = aline.Split(',');
                long bID = Convert.ToInt64(sline[1]);
                int minAlloc = Convert.ToInt32(sline[2]);
                Console.WriteLine("Setting " + sline[0] + " to " + minAlloc);

                AdminManager.BrokerAdmin.SetMinimumEngines(bID, minAlloc);

            }

            DriverManager.Disconnect();
        }

    }
}
