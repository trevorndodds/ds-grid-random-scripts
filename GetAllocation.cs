using System;
using System.IO;
using System.Linq;
using System.Text;
using DataSynapse.GridServer.Driver;
using DataSynapse.GridServer.Admin;
using DataSynapse.GridServer.Engine;

namespace GetAllocation
{
    class GetAllocation
    {
        public static void Main(string[] args)
        {

            //    Console.WriteLine("Number of command line parameters = {0}", args.Length);
            for (int i = 0; i < args.Length; i++)
            {
                Console.WriteLine("Arg[{0}] = [{1}]", i, args[i]);
            }

            if (args.Length > 0)
            {
                Console.WriteLine("Using Director: " + args[0]);
                string director = args[0];
                DriverManager.Connect(director);
            }
            else
            {
                string director = Convert.ToString(00000000000);
                Console.WriteLine("No Args found, using default PROD connection");
                DriverManager.Connect(director);
            }

            DateTime newTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            TextWriter tf = new StreamWriter("Allocation.txt", true);

            Array Brokers = AdminManager.BrokerAdmin.GetAllBrokerInfo();
            foreach (BrokerInfo line in Brokers)
            {
                tf.WriteLine(line.Name + "," + line.BrokerId + "," + line.MinEngines);
            }

            tf.Close();

            DriverManager.Disconnect();
        }

    }
}
