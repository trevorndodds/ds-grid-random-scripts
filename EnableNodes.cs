using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DataSynapse.GridServer.Driver;
using DataSynapse.GridServer.Admin;
using System.Threading;

namespace EnableNodes
{
    class EnableNodes
    {
        static void Main(string[] args)
        {
            var listfile = "";
            for (int i = 0; i < args.Length; i++)
            {
                Console.WriteLine("Arg[{0}] = [{1}]", i, args[i]);
            }

            if (args.Length > 0)
            {
                //  Console.WriteLine("Args Found: " + args[0]);
                listfile = args[0];
            }
            else
            {
                Console.WriteLine("Usage: EnableNodes.exe <listfile>.txt");
                Environment.Exit(0);
            }

            string proddirector = Convert.ToString(00000000);
            Console.WriteLine("Connecting to GRID");
            DriverManager.Connect(proddirector);

            EngineDaemonAdmin enginedaemonAdmin = AdminManager.EngineDaemonAdmin;
            EngineDaemonInfo[] engineDaemonInfos = enginedaemonAdmin.GetAllEngineDaemonInfo();

            Array allinfo = AdminManager.EngineDaemonAdmin.GetAllEngineDaemonInfo();

            if (allinfo == null)
            {
                Console.WriteLine("No Servers found in Array");
                return;
            }

            string[] file = System.IO.File.ReadAllLines(@listfile);
            foreach (string server in file)
            {
                // Console.WriteLine("Enabling " + server);

                foreach (EngineDaemonInfo line in allinfo)
                {
                    foreach (Property a in line.Properties)
                    {
                        if (a.Name == "username")
                        {
                            if (a.Value == server)
                            {
                                Console.WriteLine("Enabling " + a.Value + ", Current: " + line.Enabled);
                                AdminManager.EngineDaemonAdmin.SetEnabled(line.EngineId, true);

                            }
                        }
                    }

                }

            }

            DriverManager.Disconnect();
        }
    }
}
