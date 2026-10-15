// SPDX-License-Identifier: Apache-2.0
import { useQuery } from '@tanstack/react-query';
import { BASE_PARAMS } from 'lib/constants';
import { useClusters } from 'lib/hooks/api/clusters';

export function useReadOnlyGuard(clusterName?: string): boolean {
  const { data: clusters } = useClusters();
  const { data: policy, isError } = useQuery(
    ['readonly-policy'],
    async (): Promise<{ readOnly: boolean }> => {
      const response = await fetch(`${BASE_PARAMS.basePath}/api/readonly-policy`, {
        credentials: 'include',
      });
      if (!response.ok) throw new Error('Read-only policy unavailable');
      const value = await response.json();
      if (typeof value.readOnly !== 'boolean') throw new Error('Invalid read-only policy');
      return value;
    },
    { staleTime: 0, refetchInterval: 30000, retry: false }
  );
  // Disable mutation controls while policy is unavailable. The server is authoritative.
  if (isError || !policy || policy.readOnly) return true;
  if (!clusterName) return false;
  const cluster = clusters?.find((item) => item.name === clusterName);
  return !cluster || !!cluster.readOnly;
}
