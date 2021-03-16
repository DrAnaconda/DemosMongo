using System;
using System.Collections.Generic;
using System.Text;

namespace Demos
{
    public static class AccessMaskHelper
    {
        static public long BuildMask(bool sAdmin = false, bool admin = false, bool staff = false, bool user = false)
        {
            long mask = 0;
            if (sAdmin) mask += (long)GlobalRoles.SAdmin;
            if (admin) mask += (long)GlobalRoles.Admin;
            if (staff) mask += (long)GlobalRoles.Staff;
            if (user) mask += (long)GlobalRoles.User;
            return mask;
        }
        public static bool BitmaskAnyBitSet(this long contextAccess, long requiredAccess)
        {
            if (contextAccess < 0 || requiredAccess < 0)
            {
                throw new InvalidOperationException($"You should not pass negative values for this operation");
            }
            return ((contextAccess & requiredAccess) > 0);
        }
        private static bool ThrowForFailedCheck(string uid, IEnumerable<string> failedEntitiesIds, long failedRequiredAccess, bool throwOnFailure)
        {
            var message = $"User {uid} have no access to entities: {string.Join(',', failedEntitiesIds)} with one of access type {failedRequiredAccess}";
            return throwOnFailure ? throw new ApiException(message, 403) : false;
        }
        public static bool CheckAccessToBuildings(SomePrincipalClass access, long requiredOneOfAccess, 
            bool throwOnFailure, params string[] requiredTargetEntities)
        {
            if (requiredTargetEntities == null)
                throw new ArgumentNullException(nameof(requiredTargetEntities));

            var failedChecks = new List<string>(requiredTargetEntities.Length);
            foreach (var requiredTargetEntity in requiredTargetEntities)
            {
                var cerainAccessGroup = access.FirstOrDefault(x => x.accessGroupId == requiredAccessGroup);
                if (cerainAccessGroup == null)
                {
                    failedChecks.Add(requiredTargetEntity);
                }
                else
                {
                    if (!cerainAccessGroup.accessMask.bitmaskAnyBitSet(requiredOneOfAccess))
                        failedChecks.Add(requiredTargetEntity);
                }
            }
            return failedChecks?.Count > 0 ? throwForFailedCheck(access.uid, failedChecks, requiredOneOfAccess, throwOnFailure) : true;
        }
    }
}