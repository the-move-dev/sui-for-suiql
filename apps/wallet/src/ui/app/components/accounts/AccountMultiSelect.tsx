// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import * as ToggleGroup from '@radix-ui/react-toggle-group';

import { useState } from 'react';
import { AccountMultiSelectItem } from './AccountMultiSelectItem';
import { Button } from '../../shared/ButtonUI';
import { type SerializedUIAccount } from '_src/background/accounts/Account';

type AccountMultiSelectProps = {
	accounts: SerializedUIAccount[];
	selectedAccountIDs: string[];
	onChange: (value: string[]) => void;
	enableSelectAll?: boolean;
};

export function AccountMultiSelect({
	accounts,
	selectedAccountIDs,
	onChange,
}: AccountMultiSelectProps) {
	return (
		<ToggleGroup.Root
			value={selectedAccountIDs}
			onValueChange={onChange}
			type="multiple"
			className="flex flex-col gap-3"
		>
			{accounts.map((account) => (
				<AccountMultiSelectItem
					key={account.id}
					account={account}
					state={account.selected ? 'selected' : undefined}
				/>
			))}
		</ToggleGroup.Root>
	);
}

export function AccountMultiSelectWithControls({
	selectedAccountIDs: selectedAccountsFromProps,
	accounts,
	onChange: onChangeFromProps,
}: AccountMultiSelectProps) {
	const [selectedAccounts, setSelectedAccounts] = useState(selectedAccountsFromProps);
	const onChange = (value: string[]) => {
		setSelectedAccounts(value);
		onChangeFromProps(value);
	};
	return (
		<div className="flex flex-col gap-3 [&>button]:border-none">
			<AccountMultiSelect
				selectedAccountIDs={selectedAccounts}
				accounts={accounts}
				onChange={onChange}
			/>

			<Button
				onClick={() => {
					if (selectedAccounts.length < accounts.length) {
						onChange(accounts.map((account) => account.address));
					} else {
						onChange([]);
					}
				}}
				variant="outline"
				size="xs"
				text={
					selectedAccounts.length < accounts.length
						? 'Select All Accounts'
						: 'Deselect All Accounts'
				}
			/>
		</div>
	);
}
