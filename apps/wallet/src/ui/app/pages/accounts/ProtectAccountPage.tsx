// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { useNavigate } from 'react-router-dom';
import { useOnboardingFormContext } from './ImportAccountsPage';
import { ProtectAccountForm } from '../../components/accounts/ProtectAccountForm';
import { useBackgroundClient } from '../../hooks/useBackgroundClient';
import { Heading } from '../../shared/heading';
import { Text } from '_app/shared/text';
import { entropyToSerialized, mnemonicToEntropy } from '_src/shared/utils/bip39';

export function ProtectAccountPage() {
	const backgroundClient = useBackgroundClient();
	const [values] = useOnboardingFormContext();
	const navigate = useNavigate();
	return (
		<div className="rounded-20 bg-sui-lightest shadow-wallet-content flex flex-col items-center px-6 py-10 h-full">
			<Text variant="caption" color="steel-dark" weight="semibold">
				Wallet Setup
			</Text>
			<div className="text-center mt-2.5">
				<Heading variant="heading1" color="gray-90" as="h1" weight="bold">
					Protect Account with a Password Lock
				</Heading>
			</div>
			<div className="mt-6 w-full grow">
				<ProtectAccountForm
					cancelButtonText="Back"
					submitButtonText="Create Wallet"
					onSubmit={async (formValues) => {
						const mnemonic = values.recoveryPhrase.join(' ');
						const accountSource = await backgroundClient.createMnemonicAccountSource({
							password: formValues.password,
							entropy: entropyToSerialized(mnemonicToEntropy(mnemonic)),
						});
						await backgroundClient.unlockAccountSourceOrAccount({
							password: formValues.password,
							id: accountSource.id,
						});
						await backgroundClient.createAccounts({
							type: 'mnemonic-derived',
							sourceID: accountSource.id,
						});
						navigate('/tokens');
					}}
				/>
			</div>
		</div>
	);
}
