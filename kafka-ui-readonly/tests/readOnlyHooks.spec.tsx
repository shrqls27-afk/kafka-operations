// SPDX-License-Identifier: Apache-2.0
import React from 'react';
import { renderHook } from '@testing-library/react';
import { Action, ResourceType } from 'generated-sources';
import { UserInfoRolesAccessContext } from 'components/contexts/UserInfoRolesAccessContext';
import { usePermission } from 'lib/hooks/usePermission';
import { useCreatePermission } from 'lib/hooks/useCreatePermisson';
import { useReadOnlyGuard } from 'lib/hooks/useReadOnlyGuard';

jest.mock('lib/hooks/useReadOnlyGuard', () => ({useReadOnlyGuard: jest.fn()}));
jest.mock('lib/hooks/useAppParams', () => ({
  __esModule: true, default: () => ({clusterName: 'example'})
}));
const wrapper = ({children}: React.PropsWithChildren<unknown>) => (
  <UserInfoRolesAccessContext.Provider value={{username:'example',roles:new Map(),rbacFlag:false}}>
    {children}
  </UserInfoRolesAccessContext.Provider>
);
beforeEach(() => (useReadOnlyGuard as jest.Mock).mockReturnValue(true));
it('disables broker edit with RBAC disabled', () => {
  const {result}=renderHook(() => usePermission(ResourceType.CLUSTERCONFIG,Action.EDIT),{wrapper});
  expect(result.current).toBe(false);
});
it('disables topic creation with RBAC disabled', () => {
  const {result}=renderHook(() => useCreatePermission(ResourceType.TOPIC),{wrapper});
  expect(result.current).toBe(false);
});
it('preserves view with RBAC disabled', () => {
  const {result}=renderHook(() => usePermission(ResourceType.CLUSTERCONFIG,Action.VIEW),{wrapper});
  expect(result.current).toBe(true);
});
it('preserves original permission when readonly is off', () => {
  (useReadOnlyGuard as jest.Mock).mockReturnValue(false);
  const {result}=renderHook(() => usePermission(ResourceType.CLUSTERCONFIG,Action.EDIT),{wrapper});
  expect(result.current).toBe(true);
});
it('blocks a mixed view and edit request', () => {
  const {result}=renderHook(() => usePermission(ResourceType.TOPIC,[Action.VIEW,Action.EDIT]),{wrapper});
  expect(result.current).toBe(false);
});
