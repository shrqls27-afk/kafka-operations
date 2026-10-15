// SPDX-License-Identifier: Apache-2.0
import React from 'react';
import {renderHook, waitFor} from '@testing-library/react';
import {QueryClient, QueryClientProvider} from '@tanstack/react-query';
import {useReadOnlyGuard} from 'lib/hooks/useReadOnlyGuard';
import {useClusters} from 'lib/hooks/api/clusters';
jest.mock('lib/hooks/api/clusters', () => ({useClusters: jest.fn()}));
function wrapper({children}:React.PropsWithChildren<unknown>) {
  const [client]=React.useState(() => new QueryClient({defaultOptions:{queries:{retry:false,cacheTime:0}}}));
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}
beforeEach(() => {
  (useClusters as jest.Mock).mockReturnValue({data:[{name:'example',readOnly:false}]});
});
it('fails closed while policy is loading', () => {
  global.fetch=jest.fn(() => new Promise(() => {})) as jest.Mock;
  const {result}=renderHook(() => useReadOnlyGuard('example'),{wrapper});
  expect(result.current).toBe(true);
});
it('disables mutations for a global readonly policy', async () => {
  global.fetch=jest.fn().mockResolvedValue({ok:true,json:async()=>({readOnly:true})});
  const {result}=renderHook(() => useReadOnlyGuard('example'),{wrapper});
  await waitFor(()=>expect(global.fetch).toHaveBeenCalled());
  expect(result.current).toBe(true);
});
it('enables original controls only after a writable policy is loaded', async () => {
  global.fetch=jest.fn().mockResolvedValue({ok:true,json:async()=>({readOnly:false})});
  const {result}=renderHook(() => useReadOnlyGuard('example'),{wrapper});
  await waitFor(()=>expect(result.current).toBe(false));
});
it('respects cluster readonly despite global mode off', async () => {
  (useClusters as jest.Mock).mockReturnValue({data:[{name:'example',readOnly:true}]});
  global.fetch=jest.fn().mockResolvedValue({ok:true,json:async()=>({readOnly:false})});
  const {result}=renderHook(() => useReadOnlyGuard('example'),{wrapper});
  await waitFor(()=>expect(global.fetch).toHaveBeenCalled());
  expect(result.current).toBe(true);
});
it('fails closed on HTTP failure', async () => {
  global.fetch=jest.fn().mockResolvedValue({ok:false});
  const {result}=renderHook(() => useReadOnlyGuard('example'),{wrapper});
  await waitFor(()=>expect(global.fetch).toHaveBeenCalled());
  expect(result.current).toBe(true);
});
