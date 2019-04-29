import { NewGravatar as NewGravatarEvent, UpdatedGravatar as UpdatedGravatarEvent } from './types/Gravity/Gravity'
import { NewGravatar, UpdatedGravatar } from './types/schema'

export function handleNewGravatar(event: NewGravatarEvent): void {
  let newGravatar = new NewGravatar(event.params.id.toHex())
  newGravatar.owner = event.params.owner
  newGravatar.displayName = event.params.displayName
  newGravatar.imageUrl = event.params.imageUrl
  newGravatar.save()
}

export function handleUpdatedGravatar(event: UpdatedGravatarEvent): void {
  let updatedGravatar = new UpdatedGravatar(event.params.id.toHex());
  updatedGravatar.owner = event.params.owner
  updatedGravatar.displayName = event.params.displayName
  updatedGravatar.imageUrl = event.params.imageUrl
  updatedGravatar.save()
}
