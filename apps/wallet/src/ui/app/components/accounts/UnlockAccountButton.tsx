// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { useUnlockAccount } from './UnlockAccountContext';
import { useUnlockMutation } from '../../hooks/useUnlockMutation';
import { Button } from '../../shared/ButtonUI';
import { SocialButton } from '../../shared/SocialButton';
import { type SerializedUIAccount } from '_src/background/accounts/Account';
import { isZkAccountSerializedUI } from '_src/background/accounts/zk/ZkAccount';

export type UnlockAccountButtonProps = {
	account: SerializedUIAccount;
	title?: string;
};
export function UnlockAccountButton({
	account,
	title = 'Unlock Account',
}: UnlockAccountButtonProps) {
	const { id, isPasswordUnlockable } = account;
	const unlockMutation = useUnlockMutation();

	const { unlockAccount } = useUnlockAccount();

	if (isPasswordUnlockable) {
		return <Button text={title} onClick={() => unlockAccount(id)} />;
	}
	if (isZkAccountSerializedUI(account)) {
		return (
			<SocialButton
				provider={account.provider}
				onClick={() => {
					unlockMutation.mutate({ id });
				}}
				loading={unlockMutation.isLoading}
				showLabel
			/>
		);
	}
}
