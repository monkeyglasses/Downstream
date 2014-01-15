using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace com.bitscopic.downstream.utils
{
    public static class ThreadUtils
    {
        /// <summary>
        /// Retry an action
        /// </summary>
        /// <param name="action"></param>
        /// <param name="maxTries"></param>
        /// <param name="sleepBetweenTries"></param>
        /// <param name="throwOnError">Can be used to specifiy exception to be thrown if retry fails</param>
        public static void retryAction(Action action, Int32 maxTries, TimeSpan sleepBetweenTries, Exception throwOnError = null)
        {
            Int32 tries = 0;
            while (tries < maxTries)
            {
                try
                {
                    action.Invoke();
                    return;
                }
                catch (Exception)
                {
                    tries++;
                    //System.Console.WriteLine(String.Format("Tried and failed {0} time{1}", tries.ToString(), (tries > 1) ? "s" : ""));
                    if (tries >= maxTries)
                    {
                        if (throwOnError != null)
                        {
                            throw throwOnError;
                        }
                        else
                        {
                            throw;
                        }
                    }
                    if (sleepBetweenTries != null && sleepBetweenTries.Duration() > TimeSpan.Zero)
                    {
                        System.Threading.Thread.Sleep(sleepBetweenTries);
                    }
                }
            }
        }

        /// <summary>
        /// Retry a list of actions with a setup and teardown. Each action will be tried maxTries number of tries individually.
        /// If setup or an action in the list fails, teardown will NOT be run
        /// </summary>
        /// <param name="setup"></param>
        /// <param name="actions"></param>
        /// <param name="teardown"></param>
        /// <param name="maxTries"></param>
        /// <param name="sleepBetweenTries"></param>
        public static void retryActions(Action setup, IList<Action> actions, Action teardown, Int32 maxTries, TimeSpan sleepBetweenTries)
        {
            setup.Invoke();
            foreach (Action a in actions)
            {
                retryAction(a, maxTries, sleepBetweenTries);
            }
            teardown();
        }
    }
}
