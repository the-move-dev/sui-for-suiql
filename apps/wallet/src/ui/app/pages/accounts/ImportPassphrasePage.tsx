// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { useNavigate } from 'react-router-dom';
import { useOnboardingFormContext } from './ImportAccountsPage';
import { ImportRecoveryPhraseForm } from '../../components/accounts/ImportRecoveryPhraseForm';
import { Heading } from '../../shared/heading';
import { Text } from '_app/shared/text';

export function ImportPassphrasePage() {
	const navigate = useNavigate();
	const [, setValues] = useOnboardingFormContext();
	return (
		<div className="rounded-20 bg-sui-lightest shadow-wallet-content flex flex-col items-center px-6 py-10 h-full overflow-auto">
			<Text variant="caption" color="steel-dark" weight="semibold">
				Wallet Setup
			</Text>
			<div className="text-center mt-2.5">
				<Heading variant="heading1" color="gray-90" as="h1" weight="bold">
					Add Existing Account
				</Heading>
			</div>
			<div className="mt-6 grow flex flex-col gap-3">
				<div className="pl-2.5">
					<Text variant="pBody" color="steel-darker" weight="semibold">
						Enter your 12-word Recovery Phrase
					</Text>
				</div>
				<ImportRecoveryPhraseForm
					cancelButtonText="Cancel"
					submitButtonText="Add Account"
					onSubmit={(formValues) => {
						navigate('/accounts/protect-account');
						setValues({ ...formValues });
					}}
				/>
			</div>
		</div>
	);
}
