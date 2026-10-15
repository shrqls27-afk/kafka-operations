import { useReadOnlyGuard } from './useReadOnlyGuard';
import { useContext } from 'react';
import { ResourceType } from 'generated-sources';
import { UserInfoRolesAccessContext } from 'components/contexts/UserInfoRolesAccessContext';
import { ClusterNameRoute } from 'lib/paths';
import { isPermittedToCreate } from 'lib/permissions';

import useAppParams from './useAppParams';

export function useCreatePermission(resource: ResourceType): boolean {
  const { clusterName } = useAppParams<ClusterNameRoute>();
  const { roles, rbacFlag } = useContext(UserInfoRolesAccessContext);

  const readOnly = useReadOnlyGuard(clusterName);
  if (readOnly) return false;

  return isPermittedToCreate({ roles, resource, clusterName, rbacFlag });
}
