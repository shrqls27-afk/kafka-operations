// SPDX-License-Identifier: Apache-2.0
import React from 'react';
import {screen} from '@testing-library/dom';
import {render, WithRoute} from 'lib/testHelpers';
import {clusterBrokerConfigsPath} from 'lib/paths';
import {useBrokerConfig} from 'lib/hooks/api/brokers';
import {brokerConfigPayload} from 'lib/fixtures/brokers';
import Configs from 'components/Brokers/Broker/Configs/Configs';
import {useReadOnlyGuard} from 'lib/hooks/useReadOnlyGuard';

jest.mock('lib/hooks/useReadOnlyGuard', () => ({useReadOnlyGuard: jest.fn()}));
jest.mock('lib/hooks/api/brokers', () => ({
  useBrokerConfig: jest.fn(), useUpdateBrokerConfigByName: jest.fn()
}));
it('disables the real broker Configs Edit buttons', () => {
  (useReadOnlyGuard as jest.Mock).mockReturnValue(true);
  (useBrokerConfig as jest.Mock).mockReturnValue({data:brokerConfigPayload});
  render(<WithRoute path={clusterBrokerConfigsPath()}><Configs /></WithRoute>,
         {initialEntries:[clusterBrokerConfigsPath('example','1')]});
  const buttons=screen.getAllByLabelText('editAction');
  expect(buttons.length).toBeGreaterThan(0);
  buttons.forEach(button=>expect(button).toBeDisabled());
});
