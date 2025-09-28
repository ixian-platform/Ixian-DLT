﻿// Copyright (C) 2017-2025 Ixian
// This file is part of Ixian DLT - www.github.com/ixian-platform/Ixian-DLT
//
// Ixian DLT is free software: you can redistribute it and/or modify
// it under the terms of the MIT License as published
// by the Open Source Initiative.
//
// Ixian DLT is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// MIT License for more details.

using DLT;
using DLT.Meta;
using IXICore;
using IXICore.Meta;
using IXICore.Utils;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace DLTNode
{
    class Program
    {
        private static Node node = null;

        private static Thread mainLoopThread;

        public static bool noStart = false;

        public static bool running = false;

        static void checkRequiredFiles()
        {
            string[] critical_dlls =
            {
                "BouncyCastle.Cryptography.dll",
                "FluentCommandLineParser.dll",
                "Newtonsoft.Json.dll",
                "Open.Nat.dll"
            };

            foreach(string critical_dll in critical_dlls)
            {
                if(!File.Exists(critical_dll))
                {
                    Logging.error("Missing '{0}' in the program folder. Possibly the IXIAN archive was corrupted or incorrectly installed. Please re-download the archive from https://www.ixian.io!", critical_dll);
                    Logging.info("Press ENTER to exit.");
                    Console.ReadLine();
                    Environment.Exit(-1);
                }
            }

            // Special case for argon
            if (!File.Exists("libargon2.dll") && !File.Exists("libargon2.so") && !File.Exists("libargon2.dylib"))
            {
                Logging.error("Missing '{0}' in the program folder. Possibly the IXIAN archive was corrupted or incorrectly installed. Please re-download the archive from https://www.ixian.io!", "libargon2");
                Logging.info("Press ENTER to exit.");
                Console.ReadLine();
                Environment.Exit(-1);
            }
        }
        static void checkVCRedist()
        {
#pragma warning disable CA1416 // Validate platform compatibility
            object installed_vc_redist = Microsoft.Win32.Registry.GetValue("HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\VisualStudio\\14.0\\VC\\Runtimes\\x64", "Installed", 0);
            object installed_vc_redist_debug = Microsoft.Win32.Registry.GetValue("HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\VisualStudio\\14.0\\VC\\Runtimes\\debug\\x64", "Installed", 0);
            bool success = false;
            if ((installed_vc_redist is int && (int)installed_vc_redist > 0) || (installed_vc_redist_debug is int && (int)installed_vc_redist_debug > 0))
            {
                Logging.info("Visual C++ 2017 (v141) redistributable is already installed.");
                success = true;
            }
            else
            {
                if (!File.Exists("vc_redist.x64.exe"))
                {
                    Logging.warn("The VC++2017 redistributable file is not found. Please download the v141 version of the Visual C++ 2017 redistributable and install it manually!");
                    Logging.flush();
                    Console.WriteLine("You can download it from this URL:");
                    Console.WriteLine("https://visualstudio.microsoft.com/downloads/");
                }
                else
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine();
                    Console.WriteLine();
                    Console.WriteLine();
                    Console.WriteLine("NOTICE: In order to run this IXIAN node, Visual Studio 2017 Redistributable (v141) must be installed.");
                    Console.WriteLine("This can be done automatically by IXIAN, or, you can install it manually from this URL:");
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("https://visualstudio.microsoft.com/downloads/");
                    Console.ForegroundColor = ConsoleColor.Magenta;
                    Console.WriteLine("The installer may open a UAC (User Account Control) prompt. Please verify that the executable is signed by Microsoft Corporation before allowing it to install!");
                    Console.ResetColor();
                    Console.WriteLine();
                    Console.WriteLine("Automatically install Visual C++ 2017 redistributable? (Y/N): ");
                    ConsoleKeyInfo k = Console.ReadKey();
                    Console.WriteLine();
                    Console.WriteLine();
                    if (k.Key == ConsoleKey.Y)
                    {
                        Logging.info("Installing Visual C++ 2017 (v141) redistributable...");
                        ProcessStartInfo installer = new ProcessStartInfo("vc_redist.x64.exe");
                        installer.Arguments = "/install /passive /norestart";
                        installer.LoadUserProfile = false;
                        installer.RedirectStandardError = true;
                        installer.RedirectStandardInput = true;
                        installer.RedirectStandardOutput = true;
                        installer.UseShellExecute = false;
                        Logging.info("Starting installer. Please allow up to one minute for installation...");
                        Process p = Process.Start(installer);
                        while (!p.HasExited)
                        {
                            if (!p.WaitForExit(60000))
                            {
                                Logging.info("The install process seems to be stuck. Terminate? (Y/N): ");
                                k = Console.ReadKey();
                                if (k.Key == ConsoleKey.Y)
                                {
                                    Logging.warn("Terminating installer process...");
                                    p.Kill();
                                    Logging.warn("Process output: {0}", p.StandardOutput.ReadToEnd());
                                    Logging.warn("Process error output: {0}", p.StandardError.ReadToEnd());
                                }
                            }
                        }
                        installed_vc_redist = Microsoft.Win32.Registry.GetValue("HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\VisualStudio\\14.0\\VC\\Runtimes\\x64", "Installed", 0);
                        installed_vc_redist_debug = Microsoft.Win32.Registry.GetValue("HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\VisualStudio\\14.0\\VC\\Runtimes\\debug\\x64", "Installed", 0);
                        if ((installed_vc_redist is int && (int)installed_vc_redist > 0) || (installed_vc_redist_debug is int && (int)installed_vc_redist_debug > 0))
                        {
                            Logging.info("Visual C++ 2017 (v141) redistributable has installed successfully.");
                            success = true;
                        }
                        else
                        {
                            Logging.info("Visual C++ 2017 has failed to install. Please review the error text (if any) and install manually:");
                            Logging.warn("Process exit code: {0}.", p.ExitCode);
                            Logging.warn("Process output: {0}", p.StandardOutput.ReadToEnd());
                            Logging.warn("Process error output: {0}", p.StandardError.ReadToEnd());
                        }
                    }
                }
            }
            if (!success)
            {
                Logging.info("IXIAN requires the Visual Studio 2017 runtime for normal operation. Please ensure it is installed and then restart the program!");
                Logging.info("Press ENTER to exit.");
                Console.ReadLine();
                Environment.Exit(-1);
            }
#pragma warning restore CA1416 // Validate platform compatibility
        }

        static void Main(string[] args)
        {
            if (!Console.IsOutputRedirected)
            {
                // There are probably more problematic Console operations if we're working in stdout redirected mode, but 
                // this one is blocking automated testing.
                Console.Clear();
            }

            IXICore.Utils.ConsoleHelpers.prepareWindowsConsole();

            Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e) {
                ConsoleHelpers.verboseConsoleOutput = true;
                Logging.consoleOutput = ConsoleHelpers.verboseConsoleOutput;
                e.Cancel = true;
                IxianHandler.forceShutdown = true;
            };

            // For testing only. Run any experiments here as to not affect the infrastructure.
            // Failure of tests will result in termination of the dlt instance.
            /*if(runTests(args) == false)
            {
                return;
            }*/

            onStart(args);

            if(Node.apiServer != null)
            { 
                while (IxianHandler.forceShutdown == false)
                {
                    Thread.Sleep(1000);
                }
            }

            if (noStart == false)
            {
                ConsoleHelpers.verboseConsoleOutput = true;
                Logging.consoleOutput = ConsoleHelpers.verboseConsoleOutput;
                Console.WriteLine("Ixian DLT is stopping, please wait...");
            }

            onStop();
        }

        static void onStart(string[] args)
        {
            ConsoleHelpers.verboseConsoleOutput = true;

            Console.ForegroundColor = ConsoleColor.DarkGreen;
            Console.WriteLine(string.Format("IXIAN DLT {0} ({1})", Config.version, CoreConfig.version));
            Console.ResetColor();

            // Check for critical files in the exe dir
            checkRequiredFiles();

            // Read configuration from command line
            Config.init(args);

            if (noStart)
            {
                return;
            }

            // First create the data folder if it does not already exist
            Node.checkDataFolder();

            // Start logging
            if (!Logging.start(Config.logFolderPath, Config.logVerbosity))
            {
                IxianHandler.forceShutdown = true;
                Logging.info("Press ENTER to exit.");
                Console.ReadLine();
                return;
            }

            // Set the logging options
            Logging.setOptions(Config.maxLogSize, Config.maxLogCount);
            Logging.flush();

            // Benchmark is a special case, because it will not start any part of the node
            if (Config.benchmarkMode.Length > 0)
            {
                Benchmark.start(Config.benchmarkMode);
                noStart = true;
            }

            // Debugging option: generate wallet only and set password from commandline
            if(Config.generateWalletOnly)
            {
                noStart = true;
                if (Config.networkType != NetworkType.main)
                {
                    if (File.Exists(Config.walletFile))
                    {
                        Logging.error("Wallet file {0} already exists. Cowardly refusing to overwrite!", Config.walletFile);
                    }
                    else
                    {
                        Logging.info("Generating a new wallet.");
                        WalletStorage wst = new WalletStorage(Config.walletFile);
                        wst.generateWallet(Config.dangerCommandlinePasswordCleartextUnsafe);
                    }
                } else
                {
                    // the main reason we don't allow stuff like 'generateWallet' in mainnet, is because the resulting wallet will have to:
                    //  a. Have an empty password (easy to steal via a misconfifured file sharing program)
                    //  b. Have a predefined password (ditto)
                    //  c. Require password on the command line, which usually leads to people making things like 'start.bat' with cleartext passwords, thus defeating
                    //     wallet encryption
                    // However, it is useful to be able to spin up a lot of nodes automatically and know their wallet addresses, therefore this sort of behavior is allowed
                    //   for testnet.
                    Logging.error("Due to security reasons, the 'generateWallet' option is only valid when starting a TestNet node!");
                }
            }

            if (noStart)
            {
                Thread.Sleep(1000);
                return;
            }


            Logging.info("Starting IXIAN DLT {0} ({1})", Config.version, CoreConfig.version);
            Logging.info("Operating System is {0}", Platform.getOSNameAndVersion());
            Logging.flush();

            // Check for the right vc++ redist for the argon miner
            // Ignore if we're not on Windows
            if (Platform.onWindows())
            {
                checkVCRedist();
            }

            // Log the parameters to notice any changes
            Logging.info("Network: {0}", Config.networkType);

            if(Config.workerOnly)
                Logging.info("Miner: worker-only");

            Logging.info("Server Port: {0}", Config.serverPort);
            Logging.info("API Port: {0}", Config.apiPort);
            Logging.info("Wallet File: {0}", Config.walletFile);

            // Initialize the node
            node = new Node();

            if (noStart)
            {
                Thread.Sleep(1000);
                return;
            }

            // Start the actual DLT node
            node.start(Config.verboseOutput);

            if(mainLoopThread != null)
            {
                mainLoopThread.Interrupt();
                mainLoopThread.Join();
                mainLoopThread = null;
            }

            running = true;

            mainLoopThread = new Thread(mainLoop);
            mainLoopThread.Name = "Main_Loop_Thread";
            mainLoopThread.Start();

            if (ConsoleHelpers.verboseConsoleOutput)
                Console.WriteLine("-----------\nPress ESC key or use the /shutdown API to stop the DLT process at any time.\n");

        }

        static void mainLoop()
        {
            try
            {
                while (running)
                {
                    try
                    {
                        if (!Console.IsInputRedirected && Console.KeyAvailable)
                        {
                            ConsoleKeyInfo key = Console.ReadKey();
                            if (key.Key == ConsoleKey.V)
                            {
                                ConsoleHelpers.verboseConsoleOutput = !ConsoleHelpers.verboseConsoleOutput;
                                Logging.consoleOutput = ConsoleHelpers.verboseConsoleOutput;
                                Console.CursorVisible = ConsoleHelpers.verboseConsoleOutput;
                                if (ConsoleHelpers.verboseConsoleOutput == false)
                                    Node.statsConsoleScreen.clearScreen();
                            }
                            else if (key.Key == ConsoleKey.Escape)
                            {
                                ConsoleHelpers.verboseConsoleOutput = true;
                                Logging.consoleOutput = ConsoleHelpers.verboseConsoleOutput;
                                IxianHandler.forceShutdown = true;
                            }
                            else if (key.Key == ConsoleKey.M)
                            {
                                if (Node.miner != null)
                                    Node.miner.pause = !Node.miner.pause;

                                if (ConsoleHelpers.verboseConsoleOutput == false)
                                    Node.statsConsoleScreen.clearScreen();
                            }
                            else if (key.Key == ConsoleKey.B)
                            {
                                if (Node.miner != null)
                                {
                                    // Adjust the search mode
                                    if (Node.miner.searchMode + 1 > BlockSearchMode.random)
                                    {
                                        Node.miner.searchMode = BlockSearchMode.lowestDifficulty;
                                    }
                                    else
                                    {
                                        Node.miner.searchMode++;
                                    }

                                    // Force a new block search using the newly chosen method
                                    Node.miner.forceSearchForBlock();
                                }
                            }

                        }
                    }
                    catch (Exception e)
                    {
                        Logging.error("Exception occurred in mainLoop: " + e);
                    }
                    Thread.Sleep(1000);
                }
            }
            catch (ThreadInterruptedException)
            {

            }
            catch (Exception e)
            {
                Console.WriteLine("MainLoop exception: {0}", e);
            }
        }

        static void onStop()
        {
            running = false;

            if (noStart == false)
            {
                // Stop the DLT
                Node.stop();
            }

            // Stop logging
            Logging.flush();
            Logging.stop();

            if (noStart == false)
            {
                Console.WriteLine("");
                Console.WriteLine("Ixian DLT Node stopped.");
                Console.WriteLine("");

                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(Node.shutdownMessage);
                Console.ResetColor();
            }
        }

        static bool runTests(string[] args)
        {
            Logging.log(LogSeverity.info, "Running Tests:");

            // Create a crypto lib
            CryptoLib crypto_lib = new CryptoLib(new IXICore.BouncyCastle());
            IxianKeyPair kp = crypto_lib.generateKeys(ConsensusConfig.defaultRsaKeySize, 2);

            Logging.log(LogSeverity.info, String.Format("Public Key base64: {0}", kp.publicKeyBytes));
            Logging.log(LogSeverity.info, String.Format("Private Key base64: {0}", kp.privateKeyBytes));


            /// ECDSA Signature test
            // Generate a new signature
            byte[] signature = crypto_lib.getSignature(Encoding.UTF8.GetBytes("Hello There!"), kp.publicKeyBytes);
            Logging.log(LogSeverity.info, String.Format("Signature: {0}", signature));

            // Verify the signature
            if(crypto_lib.verifySignature(Encoding.UTF8.GetBytes("Hello There!"), kp.publicKeyBytes, signature))
            {
                Logging.log(LogSeverity.info, "SIGNATURE IS VALID");
            }

            // Try a tamper test
            if (crypto_lib.verifySignature(Encoding.UTF8.GetBytes("Hello Tamper!"), kp.publicKeyBytes, signature))
            {
                Logging.log(LogSeverity.info, "SIGNATURE IS VALID AND MATCHES ORIGINAL TEXT");
            }
            else
            {
                Logging.log(LogSeverity.info, "TAMPERED SIGNATURE OR TEXT");
            }

            // Generate a new signature for the same text
            byte[] signature2 = crypto_lib.getSignature(Encoding.UTF8.GetBytes("Hello There!"), kp.privateKeyBytes);
            Logging.log(LogSeverity.info, String.Format("Signature Again: {0}", signature2));

            // Verify the signature again
            if (crypto_lib.verifySignature(Encoding.UTF8.GetBytes("Hello There!"), kp.publicKeyBytes, signature2))
            {
                Logging.log(LogSeverity.info, "SIGNATURE IS VALID");
            }



            Logging.log(LogSeverity.info, "-------------------------");

            // Generate a mnemonic hash from a 64 character string. If the result is always the same, it works correctly.
            Mnemonic mnemonic_addr = new Mnemonic(Wordlist.English, Encoding.ASCII.GetBytes("hahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahaha"));
            Logging.log(LogSeverity.info, String.Format("Mnemonic Hashing Test: {0}", mnemonic_addr));
            Logging.log(LogSeverity.info, "-------------------------");


            // Create an address from the public key
            Address addr = new Address(kp.publicKeyBytes);
            Logging.log(LogSeverity.info, String.Format("Address generated from public key above: {0}", addr));
            Logging.log(LogSeverity.info, "-------------------------");


            // Testing sqlite wrapper
            var db = new SQLite.SQLiteConnection("storage.dat");

            // Testing internal data structures
            db.CreateTable<Block>();

            Block new_block = new Block();
            db.Insert(new_block);

            IEnumerable<Block> block_list = db.Query<Block>("select * from Block");

            if (block_list.OfType<Block>().Count() > 0)
            {
                Block first_block = block_list.FirstOrDefault();
                Logging.log(LogSeverity.info, String.Format("Stored genesis block num is: {0}", first_block.blockNum));
            }


            Logging.log(LogSeverity.info, "Tests completed successfully.\n\n");

            return true;
        }
    }
}
